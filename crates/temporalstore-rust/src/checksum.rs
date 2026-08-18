// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! CRC32C (Castagnoli) for on-disk record integrity.
//!
//! The append-only logs originally carried a truncated SHA-256 per record, and the block
//! store a full 32-byte SHA-256 per page record. Both sit on the synchronous write path, so
//! their cost is paid per write, before the durability barrier -- and a cryptographic digest
//! is the wrong tool for the job. Nothing here is defending against a forged record; the
//! threat is accidental corruption of a committed record (a flipped bit that still parses),
//! which is exactly what a CRC is for. CRC32C is the same choice the reference implementation
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
    // The reflected algorithm pre- and post-inverts; carrying the inverted form across calls
    // is what makes seeding compose exactly like one pass over the concatenation.
    let mut crc = !seed;
    for byte in bytes {
        let index = ((crc ^ u32::from(*byte)) & 0xff) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
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
