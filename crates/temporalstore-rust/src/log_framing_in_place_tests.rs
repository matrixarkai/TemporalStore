// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

#[test]
fn in_place_binary_bodies_match_existing_frames_at_length_boundaries() {
    for length in [0, 1, 125, 126, 127, 128, 255, 16_382, 16_383, 16_384, 65_536] {
        for marker in [0, 10, 27, 0xB8, 0xB9, 255] {
            let body: Vec<_> = (0..length).map(|index| (index * 73 + 19) as u8).collect();
            let mut payload = vec![marker];
            payload.extend_from_slice(&body);
            let expected = encode_frame(&payload);
            let mut actual = Vec::with_capacity(length + MAX_BINARY_HEADER_BYTES + 1);
            actual.extend_from_slice(&body);
            let original = actual.as_ptr();
            frame_binary_body_in_place(marker, &mut actual);
            assert_eq!(actual, expected, "length={length} marker={marker}");
            assert_eq!(actual.as_ptr(), original, "reserved capacity must be reused");
            let (consumed, decoded) = next_frame(&actual).unwrap().unwrap();
            assert_eq!(consumed, actual.len());
            assert_eq!(decoded, payload);
        }
    }
}

#[test]
fn in_place_binary_frames_reject_body_and_marker_corruption() {
    let mut frame = vec![1, 2, 3, 4, 5];
    frame_binary_body_in_place(0xB9, &mut frame);
    for offset in [frame.len() - 1, frame.len() - 6] {
        let mut damaged = frame.clone();
        damaged[offset] ^= 1;
        assert!(next_frame(&damaged).is_err());
    }
}
