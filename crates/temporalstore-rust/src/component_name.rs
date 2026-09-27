// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! HOW A CONTENT-DERIVED COMPONENT NAME IS SPELLED.
//!
//! A component identifies one element inside a container object -- a zset member, a set member, a
//! list entry, a point in a series, an event. Where the caller supplies the name (a hash field) it
//! is text and stays text. Where this engine DERIVES the name from content it used to spell the
//! derivation in hexadecimal, and hexadecimal is two characters for every byte:
//!
//! ```text
//!     zset        {score_bits:016x} + hex(member)      16 chars for 8 bytes, then 2x
//!     set         hex(member)                          2x
//!     list        {sequence_bits:016x}                  16 chars for 8 bytes
//!     event       {stored_key:016x}{identity:016x}      32 chars for 16 bytes, no user data
//!     series      stored_key.to_string()                up to 20 chars for 8 bytes
//! ```
//!
//! Every one of those characters is part of an `Arc<str>` allocated per element, held in the page
//! index, rendered into the stored block-ref key, hashed into the page handle, and written into
//! both durable logs. So the spelling is not a display choice, it is a per-element cost paid five
//! times over.
//!
//! # WHAT THIS MODULE IS
//!
//! One spelling, shared by every derived component, that carries the same information in fewer
//! characters and sorts the same way the hexadecimal did. It is base 64 over an alphabet whose
//! characters are in ASCII order, so a byte-wise comparison of two spellings is a comparison of
//! the values they spell -- which is what `ObjectBlockRefs::position` relies on, and what the zset
//! component's doc comment ("score bits then member, so lexical order is (score, member) order")
//! has always relied on.
//!
//! # WHY NOT RAW BYTES
//!
//! Raw bytes would be one character per byte instead of 1.334, and they cannot be used here: a
//! component is a `&str` and an `Arc<str>` from the producer all the way to both durable formats,
//! it is concatenated into [`crate::index_log::block_ref_key_from_parts`]'s colon-separated
//! `String`, and it is a msgpack `str` field in the index log and the write-ahead log. Making it
//! bytes is a change to three durable formats and to the type of eight fields; this is a change to
//! one function each. `component_name_bytes.rs` prices both, so the remainder is a measurement
//! rather than a guess.
//!
//! # WHY THE ALPHABET LOOKS LIKE THAT
//!
//! `+0123456789A..Z_a..z` is 64 characters in strictly ascending ASCII order, and it excludes
//! `:` (0x3A) and `|` (0x7C) -- the two separators the stored key builders use. It is not
//! standard base64: standard base64's `+/` sit below its digits and its uppercase sits below its
//! `+`, so standard base64 is NOT order preserving and using it here would silently reorder every
//! container's members.
//!
//! # THE HAZARD THIS CARRIES
//!
//! A hexadecimal spelling and this spelling are both valid `str`s, so a store written at one and
//! read at the other does not fail -- it mis-parses. `crate::engine::SHARD_INDEX_FORMAT_VERSION`
//! is the gate: it is bumped for this change, the served-index container refuses a mismatch by
//! name BEFORE decoding, and the payload-level check refuses by name too.

/// The alphabet, in strictly ascending ASCII order.
///
/// Ascending is the whole point: `encode` writes the most significant group first, so a byte-wise
/// `str` comparison of two encodings has the same result as comparing the values. The assertion
/// below is what keeps a future edit from breaking that silently.
const ALPHABET: &[u8; 64] = b"+0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ_abcdefghijklmnopqrstuvwxyz";

/// Ascending, and free of both stored-key separators.
const _: () = {
    let mut at = 1;
    while at < ALPHABET.len() {
        assert!(
            ALPHABET[at - 1] < ALPHABET[at],
            "the alphabet must ascend or the spelling stops preserving order"
        );
        assert!(ALPHABET[at] != b':' && ALPHABET[at] != b'|');
        at += 1;
    }
    assert!(ALPHABET[0] != b':' && ALPHABET[0] != b'|');
};

/// Characters a `u64` takes: 11 groups of six bits covers 64 with two bits of headroom.
///
/// Fixed width, unlike the decimal spelling it replaces. A variable-width decimal spelling of a
/// number does not sort in the number's order across a change of digit count -- `9999999999999`
/// sorts after `10000000000000` -- and the series component was spelled that way. Every real
/// millisecond timestamp is thirteen digits until the year 2286, so that was latent rather than
/// live; a fixed width removes it rather than moving the date.
pub(crate) const U64_CHARS: usize = 11;

/// Where `decode_char` sends a character that is not in the alphabet.
const INVALID: u8 = 0xFF;

/// The alphabet inverted, built at compile time so a decode is a table read.
const DECODE: [u8; 256] = {
    let mut table = [INVALID; 256];
    let mut at = 0;
    while at < ALPHABET.len() {
        table[ALPHABET[at] as usize] = at as u8;
        at += 1;
    }
    table
};

/// Append `value` as exactly [`U64_CHARS`] characters, most significant group first.
pub(crate) fn push_u64(out: &mut String, value: u64) {
    // 11 groups of 6 bits is 66, so the top group carries the top four bits and the value is
    // shifted left by two to sit flush against the group boundary.
    let padded = (value as u128) << 2;
    for group in (0..U64_CHARS).rev() {
        let six = ((padded >> (group * 6)) & 0x3F) as usize;
        out.push(ALPHABET[six] as char);
    }
}

/// `value` on its own.
pub(crate) fn u64_text(value: u64) -> String {
    let mut text = String::with_capacity(U64_CHARS);
    push_u64(&mut text, value);
    text
}

/// The number [`push_u64`] wrote, or `None` for anything that is not exactly that.
///
/// Refuses a wrong length and refuses a character outside the alphabet, so a hexadecimal spelling
/// of the same number does not parse as a different number -- it does not parse at all. That is
/// the only reason this returns an `Option` rather than saturating.
pub(crate) fn parse_u64(text: &str) -> Option<u64> {
    if text.len() != U64_CHARS {
        return None;
    }
    let mut padded: u128 = 0;
    for byte in text.as_bytes() {
        let six = DECODE[*byte as usize];
        if six == INVALID {
            return None;
        }
        padded = (padded << 6) | u128::from(six);
    }
    // The two pad bits must be zero, or this is not something `push_u64` produced.
    if padded & 0b11 != 0 {
        return None;
    }
    u64::try_from(padded >> 2).ok()
}

/// Characters `bytes` takes: four per three bytes, the tail not padded.
pub(crate) fn bytes_chars(byte_len: usize) -> usize {
    byte_len / 3 * 4 + match byte_len % 3 {
        0 => 0,
        1 => 2,
        other => {
            debug_assert_eq!(other, 2);
            3
        }
    }
}

/// Append `bytes`, four characters per three bytes, the tail unpadded.
///
/// Order preserving over byte strings, including strings of different lengths: a shorter string
/// encodes to a prefix-or-less of what any extension of it encodes to, because the trailing group
/// of a short tail is zero-filled and the alphabet ascends. `the_new_spelling_sorts_exactly_as_the
/// _hexadecimal_one_did` is the randomised check.
pub(crate) fn push_bytes(out: &mut String, bytes: &[u8]) {
    let mut chunks = bytes.chunks_exact(3);
    for chunk in &mut chunks {
        let word = (u32::from(chunk[0]) << 16) | (u32::from(chunk[1]) << 8) | u32::from(chunk[2]);
        for group in (0..4).rev() {
            out.push(ALPHABET[((word >> (group * 6)) & 0x3F) as usize] as char);
        }
    }
    match chunks.remainder() {
        [] => {}
        [one] => {
            out.push(ALPHABET[usize::from(one >> 2)] as char);
            out.push(ALPHABET[usize::from((one & 0b11) << 4)] as char);
        }
        [one, two] => {
            out.push(ALPHABET[usize::from(one >> 2)] as char);
            out.push(ALPHABET[usize::from(((one & 0b11) << 4) | (two >> 4))] as char);
            out.push(ALPHABET[usize::from((two & 0x0F) << 2)] as char);
        }
        // `chunks_exact` cannot leave three or more.
        _ => unreachable!("a remainder of a three-byte chunking is under three bytes"),
    }
}

/// `bytes` on their own.
pub(crate) fn bytes_text(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes_chars(bytes.len()));
    push_bytes(&mut text, bytes);
    text
}

/// The bytes [`push_bytes`] wrote, or `None` for anything that is not exactly that.
pub(crate) fn parse_bytes(text: &str) -> Option<Vec<u8>> {
    // A remainder of one character carries at most six bits, which is not a byte: no encoding
    // this module produces has one.
    if text.len() % 4 == 1 {
        return None;
    }
    let mut six = Vec::with_capacity(text.len());
    for byte in text.as_bytes() {
        let value = DECODE[*byte as usize];
        if value == INVALID {
            return None;
        }
        six.push(value);
    }
    let mut out = Vec::with_capacity(six.len() / 4 * 3 + 2);
    let mut groups = six.chunks_exact(4);
    for group in &mut groups {
        out.push((group[0] << 2) | (group[1] >> 4));
        out.push((group[1] << 4) | (group[2] >> 2));
        out.push((group[2] << 6) | group[3]);
    }
    match groups.remainder() {
        [] => {}
        [one, two] => {
            if two & 0x0F != 0 {
                return None;
            }
            out.push((one << 2) | (two >> 4));
        }
        [one, two, three] => {
            if three & 0b11 != 0 {
                return None;
            }
            out.push((one << 2) | (two >> 4));
            out.push((two << 4) | (three >> 2));
        }
        // The `% 4 == 1` guard above rejected the only other remainder length.
        _ => unreachable!("a remainder of a four-group chunking is under four groups"),
    }
    Some(out)
}

/// THE SPELLING THIS REPLACED, AND IT IS NOT A SECOND FORMAT.
///
/// `#[cfg(test)]`, deliberately and load-bearingly: THERE IS ONE VERSION. A shipped binary holds
/// exactly one spelling of a derived component name and no path that reads another, so nothing here
/// can be reached by the serving or replay code even by mistake. What it exists for is two
/// measurements that have to render what the old spelling WOULD have been for the same values --
/// `component_name_bytes.rs`'s saving column, and the plant that proves a store at the old spelling
/// is refused rather than mis-read. A hand-rolled copy inside a test would be a copy that drifts
/// from what actually shipped; a `#[cfg(test)]` module cannot become a compatibility window.
#[cfg(test)]
pub(crate) mod legacy {
    /// `{value:016x}` -- what a derived `u64` component used to be.
    pub(crate) fn u64_text(value: u64) -> String {
        format!("{value:016x}")
    }

    /// `hex::encode(bytes)` -- what a derived byte component used to be.
    pub(crate) fn bytes_text(bytes: &[u8]) -> String {
        hex::encode(bytes)
    }

    /// `stored_key.to_string()` -- what a series component used to be. Decimal, not hexadecimal,
    /// and variable width: the two arms of `timestamped_component` did not share an alphabet.
    pub(crate) fn decimal_text(value: u64) -> String {
        value.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// EVERY SPELLING ROUND-TRIPS, AND NOTHING ELSE PARSES AS ONE.
    ///
    /// The second half is the one that matters. A parser that accepts a spelling it did not produce
    /// is what turns a format change into a silent mis-read, and every character of the hexadecimal
    /// spelling this replaced is a legal character here -- so the refusals are asserted by name
    /// rather than left to the round trip to imply.
    #[test]
    fn a_spelled_number_comes_back_and_nothing_else_parses_as_one() {
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        let mut values = vec![0u64, 1, 2, 3, u64::MAX, u64::MAX - 1, 1 << 63, (1 << 63) - 1];
        for _ in 0..2_000 {
            seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
            values.push(seed);
        }
        for value in &values {
            let text = u64_text(*value);
            assert_eq!(
                text.len(),
                U64_CHARS,
                "{value} spelled as {} characters, not {U64_CHARS}",
                text.len()
            );
            assert!(
                text.is_ascii(),
                "{value} spelled as {text:?}, which is not ASCII; the spelling has to be a legal \
                 `str` and a legal part of the stored block-ref key"
            );
            assert!(
                !text.contains(':') && !text.contains('|'),
                "{value} spelled as {text:?}, which holds a stored-key separator"
            );
            assert_eq!(
                parse_u64(&text),
                Some(*value),
                "{value} spelled as {text:?} and did not come back"
            );
        }
        println!(
            "[u64] {} values round-tripped at {U64_CHARS} characters",
            values.len()
        );

        // A wrong width is refused, both ways.
        for width in [0usize, 1, U64_CHARS - 1, U64_CHARS + 1, 16, 20, 32] {
            let text: String = std::iter::repeat('0').take(width).collect();
            assert_eq!(
                parse_u64(&text),
                None,
                "a {width}-character string parsed as a number, where only {U64_CHARS} may"
            );
        }
        // A character outside the alphabet is refused. `:` is a stored-key separator and `/` sits
        // below the digits in ASCII, so both are what a careless alphabet would let through.
        for bad in [':', '/', '.', '!', '~'] {
            let mut text = u64_text(12_345);
            text.pop();
            text.push(bad);
            assert_eq!(
                parse_u64(&text),
                None,
                "{text:?} parsed as a number and it holds {bad:?}, which is not in the alphabet"
            );
        }
        // The two pad bits must be zero, so a string that LOOKS well formed but sets them is
        // refused rather than rounded.
        let mut nudged = u64_text(1);
        nudged.pop();
        nudged.push(ALPHABET[1] as char);
        assert_eq!(
            parse_u64(&nudged),
            None,
            "{nudged:?} parsed, and its low pad bits are not zero; a spelling this module did not \
             write must not read as one"
        );
    }

    /// A SPELLED BYTE STRING COMES BACK, AT EVERY LENGTH AND EVERY TAIL.
    #[test]
    fn a_spelled_byte_string_comes_back_at_every_tail_length() {
        let mut seed = 0x2545_F491_4F6C_DD1Du64;
        let mut checked = 0usize;
        for length in 0..40usize {
            for _ in 0..12 {
                seed = seed.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1);
                let bytes: Vec<u8> = (0..length)
                    .map(|at| (seed >> ((at % 8) * 8)) as u8)
                    .collect();
                let text = bytes_text(&bytes);
                assert_eq!(
                    text.len(),
                    bytes_chars(bytes.len()),
                    "{} bytes spelled as {} characters, where bytes_chars says {}",
                    bytes.len(),
                    text.len(),
                    bytes_chars(bytes.len())
                );
                assert!(text.is_ascii(), "{bytes:?} spelled as non-ASCII {text:?}");
                assert_eq!(
                    parse_bytes(&text).as_deref(),
                    Some(bytes.as_slice()),
                    "{bytes:?} spelled as {text:?} and did not come back"
                );
                // Never longer than the hexadecimal it replaced, and strictly shorter from two
                // bytes up. ONE byte TIES at two characters: hexadecimal takes 2n and this takes
                // ceil(4n/3), and those are equal at n=1. The saving is a property of the width,
                // not a constant, and asserting it as a constant was wrong.
                assert!(
                    text.len() <= 2 * bytes.len(),
                    "{} bytes spelled as {} characters, above the 2x the hexadecimal took",
                    bytes.len(),
                    text.len()
                );
                if bytes.len() >= 2 {
                    assert!(
                        text.len() < 2 * bytes.len(),
                        "{} bytes spelled as {} characters, which is not under the 2x the \
                         hexadecimal spelling took",
                        bytes.len(),
                        text.len()
                    );
                }
                if bytes.len() == 1 {
                    assert_eq!(
                        text.len(),
                        2,
                        "a one-byte member spelled as {} characters; it ties hexadecimal at two",
                        text.len()
                    );
                }
                checked += 1;
            }
        }
        assert!(checked >= 400, "only {checked} byte strings were checked");
        println!("[bytes] {checked} byte strings round-tripped across lengths 0..40");

        // A remainder of one character carries six bits, which is not a byte: nothing writes it.
        assert!(parse_bytes("+").is_none(), "a one-character spelling parsed");
        assert!(
            parse_bytes("+++++").is_none(),
            "a five-character spelling parsed"
        );
        // A non-zero tail is a spelling this module did not write.
        let non_zero_tail = format!("{}{}", ALPHABET[0] as char, ALPHABET[1] as char);
        assert!(
            parse_bytes(&non_zero_tail).is_none(),
            "{non_zero_tail:?} parsed, and its tail bits are not zero"
        );
        // And a character outside the alphabet is refused.
        assert!(parse_bytes("++:+").is_none(), "a spelling holding `:` parsed");
    }

    /// THE ALPHABET IS THE WHOLE ORDER GUARANTEE, so it is checked as data and not as a comment.
    ///
    /// The const assertion beside the table checks it ascends at compile time. This checks the two
    /// properties a reader of the module would otherwise have to take on trust: that it is exactly
    /// sixty-four distinct characters, and that standard base64's alphabet -- the obvious thing to
    /// reach for -- would NOT have worked, so the hand-rolled table is not a matter of taste.
    #[test]
    fn the_alphabet_is_sixty_four_ascending_characters_and_standard_base64_is_not() {
        let distinct: std::collections::BTreeSet<u8> = ALPHABET.iter().copied().collect();
        assert_eq!(
            distinct.len(),
            64,
            "the alphabet holds {} distinct characters",
            distinct.len()
        );
        for byte in ALPHABET {
            assert!(
                byte.is_ascii_graphic(),
                "{byte:?} is not a printable ASCII character"
            );
        }
        const STANDARD: &[u8; 64] =
            b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
        let ascending = STANDARD.windows(2).all(|pair| pair[0] < pair[1]);
        assert!(
            !ascending,
            "standard base64's alphabet ascends, so the reason this module rolls its own is gone \
             and the module's doc comment is now wrong"
        );
        println!(
            "[alphabet] {} distinct ascending characters; standard base64's does not ascend, which \
             is why it is not used",
            distinct.len()
        );
    }
}
