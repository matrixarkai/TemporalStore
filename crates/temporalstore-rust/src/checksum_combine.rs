// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::CRC32C_POLYNOMIAL;

type Operator = [u32; 32];

const fn apply(operator: &Operator, mut value: u32) -> u32 {
    let mut result = 0;
    let mut bit = 0;
    while value != 0 {
        if value & 1 != 0 {
            result ^= operator[bit];
        }
        value >>= 1;
        bit += 1;
    }
    result
}

const fn square(operator: Operator) -> Operator {
    let mut result = [0; 32];
    let mut bit = 0;
    while bit < 32 {
        result[bit] = apply(&operator, operator[bit]);
        bit += 1;
    }
    result
}

const fn byte_operators() -> [Operator; usize::BITS as usize] {
    let mut bit_operator = [0; 32];
    bit_operator[0] = CRC32C_POLYNOMIAL;
    let mut bit = 1;
    while bit < 32 {
        bit_operator[bit] = 1 << (bit - 1);
        bit += 1;
    }
    let mut operators = [[0; 32]; usize::BITS as usize];
    operators[0] = square(square(square(bit_operator)));
    let mut power = 1;
    while power < operators.len() {
        operators[power] = square(operators[power - 1]);
        power += 1;
    }
    operators
}

// One immutable operator for each bit in a byte length; 8 KiB on 64-bit hosts.
// Cold directory validation applies these rather than squaring matrices per frame.
static BYTE_OPERATORS: [Operator; usize::BITS as usize] = byte_operators();

/// CRC32C of two concatenated streams from their checksums and the second stream's length.
pub(crate) fn crc32c_combine(mut left: u32, right: u32, mut right_length: usize) -> u32 {
    if right_length == 0 {
        return left;
    }
    let mut power = 0;
    while right_length != 0 {
        if right_length & 1 != 0 {
            left = apply(&BYTE_OPERATORS[power], left);
        }
        right_length >>= 1;
        power += 1;
    }
    left ^ right
}

#[cfg(test)]
#[path = "checksum_combine_tests.rs"]
mod tests;
