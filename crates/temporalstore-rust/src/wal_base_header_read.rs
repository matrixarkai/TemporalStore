// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! What reading a log piece's base header COSTS, against what the header is.
//!
//! #1936 named this and did not fix it: `read_wal_base` fills 8 KiB to read a header line under
//! 100 bytes, once per piece per replay window, and priced it at 483,328 of the 34,779,419 bytes
//! a 200,000-record restore reads off its log -- 1.4%.
//!
//! A buffered reader filling its buffer is normal and often CORRECT. The question is whether
//! anything after the header read reuses the buffer. In `read_wal_base` nothing does: the
//! `BufReader` is a local, it is dropped at the end of the function, and what escapes is two
//! integers. The fill is bought and thrown away.
//!
//! DIRECTION. Reading too LITTLE on a recovery path is silent data loss -- a header that failed
//! to decode reads as a base of zero, and a base of zero resolves every log id in that piece to
//! the wrong record. Reading too much is merely slow. So the bound is not asserted by LENGTH: the
//! decoded answer is compared FIELD BY FIELD against a control that reads the way the old code
//! did, over every shape a piece can be in.

#![cfg(test)]

use crate::wal::{
    read_wal_base_bounded_for_test, read_wal_base_unbounded_for_test, wal_base_header_probe_bytes,
    wal_piece_read_counts_on_this_thread,
};

/// The longest base header this writer can ever produce, computed from the writer rather than
/// assumed: `#tsb1 ` + a checksum + a space + `u64::MAX` in decimal + a newline.
fn longest_possible_header() -> usize {
    crate::log_framing::encode_base_header(u64::MAX).len()
}

// -------------------------------------------------------------------------------------------
// THE SHAPES A PIECE CAN BE IN
//
// Every one of them is fed to BOTH readers and the two answers are compared field by field. A
// shape this list leaves out is a shape the bound was never checked on.
// -------------------------------------------------------------------------------------------

/// `(name, bytes on disk)` for every shape `read_wal_base` has to answer for.
fn piece_shapes() -> Vec<(&'static str, Vec<u8>)> {
    let header = crate::log_framing::encode_base_header(4_096);
    let big_header = crate::log_framing::encode_base_header(u64::MAX);
    let record = crate::log_framing::encode_line(br#"{"sequence":1,"payload":"x"}"#);

    let mut header_then_records = header.clone();
    for _ in 0..64 {
        header_then_records.extend_from_slice(&record);
    }

    let mut records_only = Vec::new();
    for _ in 0..64 {
        records_only.extend_from_slice(&record);
    }

    // A piece whose first line is longer than the probe window and is NOT a header. The old
    // reader reads to its newline; the bounded one stops. Both must still answer "no header".
    let mut long_first_line = vec![b'x'; wal_base_header_probe_bytes() * 4];
    long_first_line.push(b'\n');
    long_first_line.extend_from_slice(&record);

    // A piece with NO newline anywhere: the old reader reads the whole file to find out.
    let no_newline = vec![b'x'; wal_base_header_probe_bytes() * 4];

    // A first line that OPENS WITH THE HEADER MAGIC and runs past the probe window. This writer
    // cannot produce one, so it is corruption or another writer's file -- and it is the shape the
    // bounded read must NOT quietly call base zero. It is here because it is the only input that
    // reaches the fallback: without it, a fallback that never fired would pass every other shape.
    let mut long_header_like = crate::log_framing::BASE_HEADER_MAGIC.to_vec();
    long_header_like.extend(std::iter::repeat(b'z').take(wal_base_header_probe_bytes() * 3));
    long_header_like.push(b'\n');

    vec![
        ("empty", Vec::new()),
        ("header only", header.clone()),
        ("header with no trailing newline", header[..header.len() - 1].to_vec()),
        ("longest header this writer can make", big_header),
        ("header then 64 records", header_then_records),
        ("records only, no header", records_only),
        ("one record", record.clone()),
        ("a first line longer than the probe window", long_first_line),
        ("a header-like first line past the probe window", long_header_like),
        ("no newline anywhere", no_newline),
        ("a single newline", vec![b'\n']),
        ("the header magic and nothing else", b"#tsb1 ".to_vec()),
    ]
}

// -------------------------------------------------------------------------------------------

/// THE STRONG FORM: the bounded read and the read it replaces AGREE, on every shape, FIELD BY
/// FIELD.
///
/// Not by length and not by "both succeeded". `read_wal_base` answers `(base, header_len)` and
/// both fields are load-bearing -- `base` resolves log ids to records and `header_len` is where
/// a window seeks to. A bound that got `header_len` right and `base` wrong would pass every
/// assertion about cost in this file.
#[test]
fn a_bounded_base_header_read_answers_exactly_what_the_buffered_one_answered() {
    let dir = tempfile::tempdir().unwrap();
    let shapes = piece_shapes();
    assert!(
        shapes.len() >= 10,
        "APPARATUS: {} shapes is not an enumeration of what a piece can be",
        shapes.len()
    );

    let mut decoded_a_header = 0usize;
    let mut answered_no_header = 0usize;
    let mut refused = 0usize;

    for (name, bytes) in &shapes {
        let path = dir.path().join(format!("piece-{}.bin", name.replace(' ', "-")));
        std::fs::write(&path, bytes).unwrap();

        let control = read_wal_base_unbounded_for_test(&path);
        let bounded = read_wal_base_bounded_for_test(&path);

        match (&control, &bounded) {
            (Ok((control_base, control_len)), Ok((bounded_base, bounded_len))) => {
                assert_eq!(
                    control_base, bounded_base,
                    "shape {name:?}: the base the bounded read decoded is {bounded_base}, the \
                     buffered one decoded {control_base} -- a wrong base resolves every log id \
                     in this piece to the wrong record"
                );
                assert_eq!(
                    control_len, bounded_len,
                    "shape {name:?}: the header length the bounded read answered is {bounded_len}, \
                     the buffered one answered {control_len} -- that is where a window seeks to"
                );
                if *control_base > 0 {
                    decoded_a_header += 1;
                } else {
                    answered_no_header += 1;
                }
            }
            (Err(control), Err(bounded)) => {
                assert_eq!(
                    control.to_string(),
                    bounded.to_string(),
                    "shape {name:?}: the two readers refused it differently"
                );
                refused += 1;
            }
            (control, bounded) => panic!(
                "shape {name:?}: one reader succeeded and the other did not -- control \
                 {control:?}, bounded {bounded:?}"
            ),
        }
    }

    // THE DENOMINATOR. A comparison in which every shape answered "no header" would pass
    // trivially: the two readers must be seen to agree on a header they actually DECODED, and on
    // one they refused.
    assert!(
        decoded_a_header >= 3,
        "APPARATUS: only {decoded_a_header} shapes decoded a header -- the agreement is vacuous"
    );
    assert!(
        answered_no_header >= 4,
        "APPARATUS: only {answered_no_header} shapes answered no header"
    );
    // A shape both readers REFUSE is what exercises the fallback: a corrupt header has to reach
    // the decoder and be told no, not be quietly answered as a base of zero. Without this
    // denominator a fallback that never fired would satisfy every line above.
    assert!(
        refused >= 2,
        "APPARATUS: only {refused} shapes were refused -- the fallback branch is not exercised"
    );
}

/// A PIECE TRUNCATED PART-WAY THROUGH ITS HEADER IS ANSWERED IN ONE PASS OVER IT.
///
/// The bounded reader has a short-read arm: no terminator, and the read stopped before the window
/// filled, so the window IS the whole file and those bytes are exactly what the buffered reader
/// would have handed the decoder. A truncated header is that shape, and a crash between the
/// header's bytes and its newline is how one is made.
///
/// WITHOUT that arm the shape still gets the RIGHT ANSWER -- it falls to the magic fallback, which
/// reads the piece again the buffered way and decodes the same base. The mutant `a piece truncated
/// part-way through its header is called headerless` survived the field-by-field comparison for
/// exactly that reason: two paths, one answer, and nothing that compares answers can tell them
/// apart. What tells them apart is that one of them reads the piece TWICE, and that is what this
/// asserts.
#[test]
fn a_truncated_header_is_answered_without_a_second_pass_over_the_piece() {
    let dir = tempfile::tempdir().unwrap();
    let header = crate::log_framing::encode_base_header(4_096);
    let truncated = &header[..header.len() - 1];
    assert_eq!(
        truncated.last(),
        Some(&b'6'),
        "APPARATUS: the fixture cut something other than the newline"
    );
    assert!(
        truncated.len() < wal_base_header_probe_bytes(),
        "APPARATUS: the truncated header is {} bytes, not shorter than the probe window",
        truncated.len()
    );

    let path = dir.path().join("truncated.bin");
    std::fs::write(&path, truncated).unwrap();

    let (bytes_before, reads_before) = wal_piece_read_counts_on_this_thread();
    let answer = read_wal_base_bounded_for_test(&path).expect("a truncated header must decode");
    let (bytes_after, reads_after) = wal_piece_read_counts_on_this_thread();
    let reads = reads_after - reads_before;
    let bytes = bytes_after - bytes_before;

    println!(
        "a {} -byte truncated header: {reads} reads, {bytes} bytes, answer {answer:?}",
        truncated.len()
    );

    // The answer FIRST: a reader that refused this would be cheaper and would satisfy every cost
    // line below. A base of zero here would resolve every log id in the piece to the wrong record.
    assert_eq!(
        answer,
        (4_096, truncated.len() as u64),
        "a header whose newline was lost decoded as {answer:?}"
    );

    // ONE PASS: the window read, and the end-of-file probe that ends its loop. A second pass over
    // the piece -- the fallback opening and reading it again -- shows up here as four.
    assert_eq!(
        reads, 2,
        "the piece was read {reads} times to answer one truncated header; one pass is the window \
         read and the end-of-file probe that ends it"
    );
    assert_eq!(
        bytes,
        truncated.len() as u64,
        "the piece is {} bytes and {bytes} were charged",
        truncated.len()
    );
}

/// The probe window is wider than the widest header this writer can produce -- computed from the
/// WRITER, so widening the header without widening the window fails here.
///
/// This is the whole safety argument for the bound, and it is arithmetic rather than a comment:
/// a header that did not fit the window would read as "no header", which is a base of zero, which
/// is the silent direction.
#[test]
fn the_probe_window_is_wider_than_the_widest_header_the_writer_can_make() {
    let widest = longest_possible_header();
    let window = wal_base_header_probe_bytes();
    assert!(
        window > widest,
        "the base-header probe window is {window} bytes and the widest header this writer can \
         produce is {widest} -- a header that does not fit reads as a base of zero"
    );
    // Not merely wider: wide enough that a header gaining a field does not silently reach it.
    assert!(
        window >= widest * 2,
        "the probe window {window} leaves under a factor of two over the widest header {widest}"
    );
}

/// WHAT THE FILL COST, AND WHAT IT COSTS NOW.
///
/// The counter is the one inside the read itself (#1936's), so this measures the same quantity a
/// restore's report does, and the kernel is asked separately.
#[test]
fn reading_a_base_header_no_longer_buys_eight_kibibytes_and_throws_it_away() {
    let dir = tempfile::tempdir().unwrap();
    let header = crate::log_framing::encode_base_header(4_096);
    let record = crate::log_framing::encode_line(br#"{"sequence":1,"payload":"x"}"#);

    // A piece far longer than the buffer, or the fill has nothing to over-read INTO and the
    // measurement is of the fixture rather than of the reader.
    let mut bytes = header.clone();
    while bytes.len() < 64 * 1024 {
        bytes.extend_from_slice(&record);
    }
    let path = dir.path().join("piece.bin");
    std::fs::write(&path, &bytes).unwrap();
    assert!(
        bytes.len() as u64 > 8 * 1024,
        "APPARATUS: the fixture piece is {} bytes, shorter than the buffer being measured",
        bytes.len()
    );

    const CALLS: u64 = 59;

    let charged = |read: fn(&std::path::Path) -> Result<(u64, u64), crate::wal::WriteAheadLogError>| {
        let (before, reads_before) = wal_piece_read_counts_on_this_thread();
        let mut answer = (0, 0);
        for _ in 0..CALLS {
            answer = read(&path).expect("the header read under measurement must succeed");
        }
        let (after, reads_after) = wal_piece_read_counts_on_this_thread();
        (after - before, reads_after - reads_before, answer)
    };

    let (buffered_bytes, buffered_reads, buffered_answer) =
        charged(read_wal_base_unbounded_for_test);
    let (bounded_bytes, bounded_reads, bounded_answer) = charged(read_wal_base_bounded_for_test);

    println!(
        "reading one piece's base header, {CALLS} times over\n\
         {:>36}{:>14}{:>10}\n\
         {:>36}{:>14}{:>10}\n\
         {:>36}{:>14}{:>10}\n\
         {:>36}{:>14}\n",
        "", "bytes", "reads",
        "buffered, as it was", buffered_bytes, buffered_reads,
        "bounded to the header", bounded_bytes, bounded_reads,
        "the header itself", header.len() as u64 * CALLS,
    );

    // The answer did not move. Asserted BEFORE the cost, because a reader that answers wrong is
    // cheaper than one that answers right and every cost assertion below would welcome it.
    assert_eq!(
        buffered_answer, bounded_answer,
        "the two readers disagree about the piece: buffered {buffered_answer:?}, bounded \
         {bounded_answer:?}"
    );
    assert_eq!(
        buffered_answer.1,
        header.len() as u64,
        "APPARATUS: the fixture's header is {} bytes and the reader answered {}",
        header.len(),
        buffered_answer.1
    );

    // What it WAS: a full buffer per call, and the buffer is 8 KiB.
    assert_eq!(
        buffered_bytes,
        8 * 1024 * CALLS,
        "APPARATUS: the buffered reader charged {buffered_bytes} bytes over {CALLS} calls, not \
         the {} a 8 KiB fill each would be",
        8 * 1024 * CALLS
    );

    // What it IS: the header and no more. Exactly, not approximately -- the bounded read takes a
    // fixed window and the window is what it is charged.
    assert!(
        bounded_bytes <= wal_base_header_probe_bytes() as u64 * CALLS,
        "the bounded reader charged {bounded_bytes} bytes over {CALLS} calls, more than the \
         {} its own window allows",
        wal_base_header_probe_bytes() as u64 * CALLS
    );
    assert!(
        bounded_bytes * 20 < buffered_bytes,
        "the bounded reader charged {bounded_bytes} against the buffered one's {buffered_bytes} \
         -- under the twentyfold this change is for"
    );
    assert_eq!(
        bounded_reads, buffered_reads,
        "the bound must not turn one read into several: {bounded_reads} against {buffered_reads}"
    );
}
