// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CRC32C (Castagnoli) for on-disk record integrity.
//!
//! The append-only logs originally carried a truncated SHA-256 per record, and the block
//! store a full 32-byte SHA-256 per page record. Both sit on the synchronous write path, so
//! their cost is paid per write, before the durability barrier -- and a cryptographic digest
//! is the wrong tool for the job. Nothing here is defending against a forged record; the
//! threat is accidental corruption of a committed record (a flipped bit that still parses),
//! which is exactly what a CRC is for. CRC32C is the same choice this design
//! makes: a per-record CRC32C in the record header plus a running per-block CRC32C in the
//! block footer.
//!
//! Implemented table-driven and dependency-free, matching how `crc64_jones` is already
//! hand-rolled for routing. The table is built at compile time, so there is no lazy-init
//! check on the hot path.
//!
//! [`crc32c_update`] takes a seed so a checksum can be accumulated across several buffers
//! without concatenating them first.

/// Castagnoli polynomial, bit-reversed (the normal form for a reflected CRC).
const CRC32C_POLYNOMIAL: u32 = 0x82f6_3b78;

/// Byte-at-a-time lookup table, generated at compile time.
const CRC32C_TABLE: [u32; 256] = build_crc32c_table();

const fn build_crc32c_table() -> [u32; 256] {
    let mut table = [0_u32; 256];
    let mut index = 0_usize;
    while index < 256 {
        let mut entry = index as u32;
        let mut bit = 0;
        while bit < 8 {
            entry = if entry & 1 == 1 {
                (entry >> 1) ^ CRC32C_POLYNOMIAL
            } else {
                entry >> 1
            };
            bit += 1;
        }
        table[index] = entry;
        index += 1;
    }
    table
}

/// CRC32C of `bytes`.
pub fn crc32c(bytes: &[u8]) -> u32 {
    crc32c_update(0, bytes)
}

/// Continue a CRC32C over `bytes`, starting from `seed` (0 for a fresh checksum).
///
/// Seeding lets a caller checksum a header and a payload separately, or accumulate across a
/// block, without building a combined buffer first.
pub fn crc32c_update(seed: u32, bytes: &[u8]) -> u32 {
    // CRC32C is the one checksum with an instruction behind it, and it is the checksum every
    // record pays on the way to disk and again on the way back. The table below is the portable
    // answer; where the processor has the instruction it takes eight bytes at a time instead of
    // one, which is worth a branch to find out.
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("sse4.2") {
            // SAFETY: reached only when the runtime check above says the instruction exists.
            return unsafe { crc32c_update_sse42(seed, bytes) };
        }
    }
    crc32c_update_table(seed, bytes)
}

/// The portable CRC32C, a byte at a time through the table.
///
/// Its own function rather than inlined into the dispatch, so the tests can hold it against the
/// accelerated one on a machine that would otherwise only ever run the fast path.
fn crc32c_update_table(seed: u32, bytes: &[u8]) -> u32 {
    // The reflected algorithm pre- and post-inverts; carrying the inverted form across calls
    // is what makes seeding compose exactly like one pass over the concatenation.
    let mut crc = !seed;
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    !crc
}

/// The same CRC32C, spent through the SSE4.2 instruction.
///
/// `_mm_crc32_u64` computes the same reflected Castagnoli CRC the table does, which is why the
/// pre- and post-inversion is identical here: this is a different way to spend the same
/// arithmetic, not a different checksum. The differential test is what holds that claim up, since
/// a checksum disagreeing by one bit would reject every record already on disk.
#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.2")]
unsafe fn crc32c_update_sse42(seed: u32, bytes: &[u8]) -> u32 {
    use std::arch::x86_64::{_mm_crc32_u64, _mm_crc32_u8};

    let mut crc = !seed;
    let split = bytes.len() - bytes.len() % 8;
    let (wide, tail) = bytes.split_at(split);

    let mut wide_crc = u64::from(crc);
    for chunk in wide.chunks_exact(8) {
        let word = u64::from_le_bytes(chunk.try_into().expect("chunks_exact(8) yields 8 bytes"));
        wide_crc = _mm_crc32_u64(wide_crc, word);
    }
    crc = wide_crc as u32;

    for byte in tail {
        crc = _mm_crc32_u8(crc, *byte);
    }
    !crc
}

/// CRC32C rendered as 8 lowercase hex characters, for the text log framing.
pub fn crc32c_hex(bytes: &[u8]) -> String {
    format!("{:08x}", crc32c(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The accelerated path and the table agree, byte for byte, at every length that matters.
    ///
    /// This is the test the change rests on. The instruction is only a faster way to spend the
    /// same arithmetic if it produces the same number; if it did not, the engine would reject
    /// every record already written. Lengths 0..=200 cover the tail handling on both sides of the
    /// eight-byte step, and the seeded pass covers the composing form the block writer uses.
    #[test]
    #[cfg(target_arch = "x86_64")]
    fn the_accelerated_checksum_is_the_same_checksum() {
        if !std::arch::is_x86_feature_detected!("sse4.2") {
            // Nothing to compare on a machine without the instruction; the table is the only path.
            return;
        }
        // Fixed rather than random, so a failure is reproducible, and varying with the index so
        // the table is exercised rather than one entry repeated.
        let payload: Vec<u8> = (0..4096u32).map(|i| (i * 31 + i / 7) as u8).collect();

        for len in 0..=200usize {
            let slice = &payload[..len];
            // SAFETY: guarded by the feature check above.
            let fast = unsafe { crc32c_update_sse42(0, slice) };
            assert_eq!(fast, crc32c_update_table(0, slice), "unseeded, len {len}");

            for seed in [1u32, 0xffff_ffff, 0x1234_5678] {
                // SAFETY: guarded by the feature check above.
                let fast = unsafe { crc32c_update_sse42(seed, slice) };
                assert_eq!(fast, crc32c_update_table(seed, slice), "seed {seed:#x}, len {len}");
            }
        }

        for len in [1024usize, 4096] {
            let slice = &payload[..len];
            // SAFETY: guarded by the feature check above.
            let fast = unsafe { crc32c_update_sse42(0, slice) };
            assert_eq!(fast, crc32c_update_table(0, slice), "len {len}");
        }
    }

    /// The portable path still matches the published vector on its own.
    ///
    /// `matches_published_castagnoli_check_vectors` goes through the dispatch, so on a machine
    /// with the instruction it now proves the accelerated path and says nothing about the table.
    /// The fallback is what runs everywhere else, so it is held against the same vector directly.
    #[test]
    fn the_portable_checksum_still_matches_the_published_vector() {
        assert_eq!(crc32c_update_table(0, b"123456789"), 0xe306_9283);
        assert_eq!(crc32c_update_table(0, b""), 0);
    }

    /// What the instruction is worth, in bytes per second over a record-sized buffer.
    ///
    /// Minimum of several passes rather than a mean: this box is shared, so the fastest run is the
    /// one least interrupted, and a mean would report the neighbours rather than the code.
    #[test]
    #[ignore]
    #[cfg(target_arch = "x86_64")]
    fn what_the_checksum_instruction_is_worth() {
        if !std::arch::is_x86_feature_detected!("sse4.2") {
            println!("  no sse4.2 on this machine; nothing to compare");
            return;
        }
        for size in [512usize, 4096, 131_072] {
            let payload: Vec<u8> = (0..size as u32).map(|i| (i * 31 + i / 7) as u8).collect();
            let rounds = 64usize;

            let mut table_best = f64::MAX;
            let mut fast_best = f64::MAX;
            for _ in 0..7 {
                // Each round is seeded with the previous round's answer, and the buffer goes
                // through `black_box` on the way in. Both are load-bearing: the checksum is a pure
                // function of loop-invariant arguments, so without a dependency the optimiser
                // hoists it out and computes it once. That first reported 443 GB/s -- past memory
                // bandwidth, which is how it was caught.
                let start = std::time::Instant::now();
                let mut acc = 0u32;
                for _ in 0..rounds {
                    acc = crc32c_update_table(acc, std::hint::black_box(&payload));
                }
                std::hint::black_box(acc);
                table_best = table_best.min(start.elapsed().as_secs_f64());

                let start = std::time::Instant::now();
                let mut acc = 0u32;
                for _ in 0..rounds {
                    // SAFETY: guarded by the feature check above.
                    acc = unsafe { crc32c_update_sse42(acc, std::hint::black_box(&payload)) };
                }
                std::hint::black_box(acc);
                fast_best = fast_best.min(start.elapsed().as_secs_f64());
            }

            let bytes = (size * rounds) as f64;
            println!(
                "  CRC {size:>7} B | table {:>7.2} MB/s | instruction {:>8.2} MB/s | {:>5.1}x",
                bytes / table_best / 1e6,
                bytes / fast_best / 1e6,
                table_best / fast_best,
            );
        }
    }

    #[test]
    fn matches_published_castagnoli_check_vectors() {
        // The standard CRC-32/ISCSI check value: CRC32C("123456789") == 0xE3069283.
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
        assert_eq!(crc32c(b""), 0x0000_0000);
        assert_eq!(crc32c(b"a"), 0xc1d0_4330);
        assert_eq!(crc32c(b"foo"), 0xcfc4_ae1d);
    }

    #[test]
    fn seeding_composes_like_one_pass_over_the_concatenation() {
        let whole = b"the quick brown fox jumps over the lazy dog";
        let (head, tail) = whole.split_at(11);
        assert_eq!(crc32c_update(crc32c(head), tail), crc32c(whole));
    }

    #[test]
    fn detects_a_value_preserving_single_bit_flip() {
        // The corruption that motivated framing in the first place: a flipped digit that
        // still parses as valid JSON.
        let original = br#"{"sequence":42}"#;
        let flipped = br#"{"sequence":49}"#;
        assert_ne!(crc32c(original), crc32c(flipped));
    }

    #[test]
    fn hex_rendering_is_fixed_width() {
        // A checksum with leading zero bytes must still occupy 8 characters, or the
        // space-delimited framing would mis-parse.
        assert_eq!(crc32c_hex(b"").len(), 8);
        assert_eq!(crc32c_hex(b""), "00000000");
        assert_eq!(crc32c_hex(b"123456789"), "e3069283");
    }
}
