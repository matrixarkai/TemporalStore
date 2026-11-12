// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use crate::checksum::crc32c;

fn data(length: usize, seed: u32) -> Vec<u8> {
    (0..length)
        .map(|i| (i as u32 * 31 + seed + i as u32 / 7) as u8)
        .collect()
}

#[test]
fn composition_matches_checksumming_concatenated_bytes() {
    for left_length in [0, 1, 7, 16, 31, 128, 4095, 8192] {
        let left = data(left_length, 3);
        for right_length in [0, 1, 2, 7, 8, 15, 16, 31, 127, 255, 256, 1023, 4096, 8192] {
            let right = data(right_length, 17);
            let mut all = left.clone();
            all.extend_from_slice(&right);
            assert_eq!(
                crc32c_combine(crc32c(&left), crc32c(&right), right_length),
                crc32c(&all),
                "left={left_length} right={right_length}"
            );
        }
    }
}

#[test]
fn composition_matches_existing_library_across_the_length_domain() {
    let seeds = [0, 1, u32::MAX, 0x1234_5678, 0x9abc_def0];
    let mut lengths: Vec<usize> = (0..=256).collect();
    for bit in 0..usize::BITS {
        let length = 1usize << bit;
        lengths.extend([length - 1, length, length.saturating_add(1)]);
    }
    let mut random = 0xe12f_987a_2b6d_51c3u64;
    for _ in 0..128 {
        random ^= random << 13;
        random ^= random >> 7;
        random ^= random << 17;
        lengths.push(random as usize);
    }
    lengths.push(usize::MAX);
    for length in lengths {
        for left in seeds {
            let right = left.rotate_left(13) ^ 0xa5c3_17f9;
            assert_eq!(
                crc32c_combine(left, right, length),
                crc32c::crc32c_combine(left, right, length),
                "left={left:#x} right={right:#x} length={length}"
            );
        }
    }
}

#[test]
fn composition_preserves_stream_grouping() {
    for length in 0..96 {
        let a = data(length, 11);
        let b = data(length * 3 + 1, 31);
        let c = data(length * 7, 47);
        let ab = crc32c_combine(crc32c(&a), crc32c(&b), b.len());
        let bc = crc32c_combine(crc32c(&b), crc32c(&c), c.len());
        let joined = [a, b.clone(), c.clone()].concat();
        assert_eq!(crc32c_combine(ab, crc32c(&c), c.len()), crc32c(&joined));
        assert_eq!(
            crc32c_combine(crc32c(&joined[..length]), bc, b.len() + c.len()),
            crc32c(&joined)
        );
    }
}

#[test]
fn zero_length_and_fixed_table_footprint_are_explicit() {
    for left in [0, 1, u32::MAX, 0x1234_5678] {
        assert_eq!(crc32c_combine(left, 0x9abc_def0, 0), left);
    }
    assert_eq!(
        std::mem::size_of_val(&BYTE_OPERATORS),
        usize::BITS as usize * 128
    );
}

#[test]
#[ignore]
fn composition_cost_on_directory_sized_work() {
    let frames: Vec<_> = (0..512u32)
        .map(|i| (i.wrapping_mul(0x9e37_79b9), 3000 + i as usize % 1024))
        .collect();
    let mut old_best = std::time::Duration::MAX;
    let mut new_best = std::time::Duration::MAX;
    for _ in 0..5 {
        let run = |combine: fn(u32, u32, usize) -> u32, best: &mut std::time::Duration| {
            let start = std::time::Instant::now();
            let mut sum = 0;
            for _ in 0..8 {
                for &(crc, length) in &frames {
                    sum = combine(std::hint::black_box(sum), std::hint::black_box(crc), length);
                }
            }
            let elapsed = start.elapsed();
            *best = (*best).min(elapsed);
            std::hint::black_box(sum)
        };
        assert_eq!(
            run(crc32c::crc32c_combine, &mut old_best),
            run(crc32c_combine, &mut new_best)
        );
    }
    println!(
        "CRC_COMPOSITION_COST frames=4096 old_ns={} new_ns={}",
        old_best.as_nanos(),
        new_best.as_nanos()
    );
}
