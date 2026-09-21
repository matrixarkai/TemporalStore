// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! WHAT A RESTORE READS, AND WHAT IS LEFT OVER WHEN EVERY NAMED READER IS SUBTRACTED.
//!
//! A restore brings a shard back from two logs: it replays the write-ahead log in bounded
//! windows, and it folds the index log's deltas over what the last dump reflected. #1936 put a
//! counter inside the first. This puts one inside the second, and then asks the question that
//! found both -- take the total from the KERNEL, subtract the readers the engine names, and look
//! at what is left.
//!
//! The residual is the subject. It is never the sum of the rows it audits, which is a number that
//! cannot fail; and when it reads ZERO it is distrusted, because an instrument that always
//! answered zero would say exactly that. A read of a file neither counter can see is planted
//! inside the same span and the residual is required to come back as exactly that many bytes.

#![cfg(test)]

use crate::index_log::{
    index_log_piece_read_counts_on_this_thread, set_index_log_segment_bytes_for_test, IndexItem,
    IndexItemKind, LocalIndexLogStore,
};
use crate::types::Command;
use crate::wal::{
    set_wal_segment_bytes_for_test, wal_piece_read_counts_on_this_thread, LocalWriteAheadLogStore,
};

/// Two sizes and the ratio between them. Four times the corpus.
const SMALL: usize = 2_000;
const BIG: usize = 8_000;

const VALUE_BYTES: usize = 128;
const SHARD: crate::types::ShardId = 21;

/// The window the engine replays with and the size it rolls at, both scaled down by eight, so a
/// test log of a few hundred kilobytes still crosses several windows and several pieces. What is
/// measured is windows against the log, and that is set by the RATIO, which is preserved.
const WINDOW_BYTES: u64 = 64 * 1024;
const WAL_SEGMENT_BYTES: u64 = 32 * 1024;
const INDEX_SEGMENT_BYTES: u64 = 8 * 1024;

// ---------------------------------------------------------------------------------------------
// THE INDEPENDENT INSTRUMENT
// ---------------------------------------------------------------------------------------------

#[derive(Clone, Copy, Debug)]
struct IoReading {
    rchar: u64,
    self_bytes: u64,
}

fn read_thread_io() -> Option<IoReading> {
    let text = std::fs::read_to_string("/proc/thread-self/io").ok()?;
    let mut rchar = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("rchar:") {
            rchar = rest.trim().parse().ok();
        }
    }
    Some(IoReading {
        rchar: rchar?,
        self_bytes: text.len() as u64,
    })
}

/// `/proc/thread-self/io`, not `/proc/self/io`: the process counter charges a span whatever
/// another thread happened to read, and a restore runs on one thread while the suite does not.
fn kernel_read_span<T>(work: impl FnOnce() -> T) -> (T, Option<u64>) {
    let before = read_thread_io();
    let value = work();
    let after = read_thread_io();
    let rchar = before
        .zip(after)
        .map(|(before, after)| (after.rchar - before.rchar).saturating_sub(before.self_bytes));
    (value, rchar)
}

// ---------------------------------------------------------------------------------------------
// THE FIXTURE
// ---------------------------------------------------------------------------------------------

fn incompressible(len: usize, seed: u64) -> Vec<u8> {
    let mut state = 0x2545_F491_4F6C_DD1D_u64
        ^ (len as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        ^ seed.wrapping_mul(0xD1B5_4A32_D192_ED03);
    (0..len)
        .map(|_| {
            state ^= state >> 12;
            state ^= state << 25;
            state ^= state >> 27;
            state.wrapping_mul(0x2545_F491_4F6C_DD1D) as u8
        })
        .collect()
}

fn index_item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Page,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.to_string(),
        model_id: "m".to_string(),
        component: None,
        object_id: 1,
        block_id: 0,
        address: None,
        size: 8,
        in_log: false,
        deleted: false,
    }
}

/// Put the two thresholds back on drop, panic included.
struct Thresholds;
impl Drop for Thresholds {
    fn drop(&mut self) {
        set_wal_segment_bytes_for_test(None);
        set_index_log_segment_bytes_for_test(None);
    }
}

struct Corpus {
    _wal_dir: tempfile::TempDir,
    _index_dir: tempfile::TempDir,
    wal: LocalWriteAheadLogStore,
    index: LocalIndexLogStore,
    wal_bytes: u64,
    wal_pieces: usize,
    index_bytes: u64,
    index_pieces: usize,
}

fn build(records: usize) -> Corpus {
    set_wal_segment_bytes_for_test(Some(WAL_SEGMENT_BYTES));
    set_index_log_segment_bytes_for_test(Some(INDEX_SEGMENT_BYTES));

    let wal_dir = tempfile::tempdir().unwrap();
    let index_dir = tempfile::tempdir().unwrap();
    let wal = LocalWriteAheadLogStore::new(wal_dir.path());
    let index = LocalIndexLogStore::new(index_dir.path());

    for value in 0..records {
        wal.append(
            SHARD,
            Command::StringSet {
                key: format!("k{value:08}"),
                value: incompressible(VALUE_BYTES, value as u64),
            },
        )
        .expect("the fixture's writes must land");
        index
            .append_delta(
                SHARD,
                vec![index_item(
                    (value % 64) as u32,
                    &format!("tenant/1/object/{value:08}"),
                )],
                Vec::new(),
                Some(value as u64 + 1),
                None,
                false,
                false,
            )
            .unwrap();
    }

    let (wal_bytes, wal_pieces) = log_on_disk(wal_dir.path(), ".wal");
    let (index_bytes, index_pieces) = log_on_disk(index_dir.path(), "");

    Corpus {
        _wal_dir: wal_dir,
        _index_dir: index_dir,
        wal,
        index,
        wal_bytes,
        wal_pieces,
        index_bytes,
        index_pieces,
    }
}

/// Read back from the directory rather than assumed: a fixture that quietly produced one piece
/// would report a flat cost and be believed.
fn log_on_disk(dir: &std::path::Path, marker: &str) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut pieces = 0usize;
    let mut stack = vec![dir.to_path_buf()];
    while let Some(at) = stack.pop() {
        for entry in std::fs::read_dir(&at).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
                continue;
            }
            let name = entry.file_name().to_string_lossy().to_string();
            if name.ends_with(".lock") || !name.contains(marker) {
                continue;
            }
            pieces += 1;
            bytes += entry.metadata().map(|meta| meta.len()).unwrap_or(0);
        }
    }
    (bytes, pieces)
}

// ---------------------------------------------------------------------------------------------
// THE RESTORE UNDER MEASUREMENT
// ---------------------------------------------------------------------------------------------

#[derive(Debug)]
struct Restore {
    /// Records the write-ahead replay handed back.
    replayed: usize,
    /// Records the index-log fold handed back.
    folded: usize,
    windows: u64,
    /// Bytes charged by #1936's counter, inside the write-ahead log's own read.
    wal_bytes: u64,
    wal_reads: u64,
    /// Bytes charged by this change's counter, inside the index log's own read.
    index_bytes: u64,
    index_reads: u64,
    /// Base-header reads this restore made -- one per piece per window.
    ///
    /// Each was an 8 KiB `BufReader` fill before this change and is a bounded
    /// `WAL_BASE_HEADER_PROBE_BYTES` window after it, so this count times the difference is
    /// exactly what the bound took off this restore. Exact rather than estimated because every
    /// piece in this fixture is longer than the buffer that used to be filled, which is asserted.
    header_reads: u64,
    /// What the kernel charged this thread over the same span.
    kernel_rchar: Option<u64>,
}

impl Restore {
    fn named(&self) -> u64 {
        self.wal_bytes + self.index_bytes
    }

    /// What this restore would have read before `read_wal_base` was bounded.
    fn bytes_before_the_bound(&self) -> u64 {
        self.named()
            + self.header_reads * (8 * 1024 - crate::wal::wal_base_header_probe_bytes() as u64)
    }
    /// THE SUBJECT. The kernel's total less every reader the engine names.
    fn residual(&self) -> Option<i64> {
        self.kernel_rchar
            .map(|rchar| rchar as i64 - self.named() as i64)
    }
}

/// Replay the write-ahead log in bounded windows and fold the index log, the way a restore does,
/// counting what both read.
fn restore(corpus: &Corpus, plant: Option<&std::path::Path>) -> Restore {
    let (wal_before, wal_reads_before) = wal_piece_read_counts_on_this_thread();
    let (index_before, index_reads_before) = index_log_piece_read_counts_on_this_thread();
    crate::wal::WAL_SEGMENT_HEADER_READS.with(|reads| reads.set(0));
    let mut replayed = 0usize;
    let mut folded = 0usize;
    let mut windows = 0u64;

    let (_, kernel_rchar) = kernel_read_span(|| {
        let mut from = corpus
            .wal
            .replay_start_after_sequence(SHARD, 0)
            .expect("a replay must be able to find where to start");
        let mut verify_tail = true;
        loop {
            let (scanned, more_to_come, resume_at) = corpus
                .wal
                .scan_decoded_window(SHARD, from, WINDOW_BYTES, verify_tail)
                .expect("the replay under measurement must succeed");
            verify_tail = false;
            windows += 1;
            replayed += scanned.len();
            if !more_to_come {
                break;
            }
            assert!(
                resume_at.log_id() > from.log_id(),
                "a window that did not advance would spin"
            );
            from = resume_at;
        }
        corpus
            .index
            .for_each_delta_record_above_anchor(SHARD, 0, 0, |_| folded += 1)
            .expect("the fold under measurement must succeed");
        if let Some(path) = plant {
            let read = std::fs::read(path).expect("the planted read must succeed");
            assert!(!read.is_empty(), "the planted file is empty");
        }
    });

    let (wal_after, wal_reads_after) = wal_piece_read_counts_on_this_thread();
    let (index_after, index_reads_after) = index_log_piece_read_counts_on_this_thread();

    Restore {
        replayed,
        folded,
        windows,
        wal_bytes: wal_after - wal_before,
        wal_reads: wal_reads_after - wal_reads_before,
        index_bytes: index_after - index_before,
        index_reads: index_reads_after - index_reads_before,
        header_reads: crate::wal::WAL_SEGMENT_HEADER_READS.with(|reads| reads.get()),
        kernel_rchar,
    }
}

/// WHAT A RESTORE READS AT TWO CORPUS SIZES, AND WHAT IS LEFT UNATTRIBUTED.
///
/// Both of this restore's readers are counted now. The residual is what says whether that is the
/// whole of them, and it is taken from outside every row it audits.
#[test]
fn a_restore_leaves_nothing_unattributed_at_two_corpus_sizes() {
    let _thresholds = Thresholds;

    let small_corpus = build(SMALL);
    let big_corpus = build(BIG);
    let small = restore(&small_corpus, None);
    let big = restore(&big_corpus, None);

    println!(
        "what a restore reads, and what no reader claims\n\
         {:>38}{:>16}{:>16}{:>10}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}{:>10.3}\n\
         {:>38}{:>16}{:>16}\n\
         {:>38}{:>16}{:>16}\n",
        "", format!("{SMALL} records"), format!("{BIG} records"), "ratio",
        "the write-ahead log on disk", small_corpus.wal_bytes, big_corpus.wal_bytes,
        big_corpus.wal_bytes as f64 / small_corpus.wal_bytes as f64,
        "  in pieces", small_corpus.wal_pieces, big_corpus.wal_pieces,
        big_corpus.wal_pieces as f64 / small_corpus.wal_pieces as f64,
        "the index log on disk", small_corpus.index_bytes, big_corpus.index_bytes,
        big_corpus.index_bytes as f64 / small_corpus.index_bytes as f64,
        "  in pieces", small_corpus.index_pieces, big_corpus.index_pieces,
        big_corpus.index_pieces as f64 / small_corpus.index_pieces as f64,
        "replay windows", small.windows, big.windows,
        big.windows as f64 / small.windows as f64,
        "the write-ahead reader (#1936)", small.wal_bytes, big.wal_bytes,
        big.wal_bytes as f64 / small.wal_bytes as f64,
        "  in reads", small.wal_reads, big.wal_reads,
        big.wal_reads as f64 / small.wal_reads as f64,
        "THE INDEX-LOG READER (this change)", small.index_bytes, big.index_bytes,
        big.index_bytes as f64 / small.index_bytes as f64,
        "  in reads", small.index_reads, big.index_reads,
        big.index_reads as f64 / small.index_reads as f64,
        "kernel rchar for this thread",
        small.kernel_rchar.unwrap_or(0), big.kernel_rchar.unwrap_or(0),
        "RESIDUAL (kernel less both readers)",
        small.residual().unwrap_or(i64::MIN), big.residual().unwrap_or(i64::MIN),
    );

    // DENOMINATORS FIRST.
    assert_eq!(
        (small.replayed, big.replayed),
        (SMALL, BIG),
        "APPARATUS: the replays handed back {} and {} records",
        small.replayed,
        big.replayed
    );
    assert_eq!(
        (small.folded, big.folded),
        (SMALL, BIG),
        "APPARATUS: the folds handed back {} and {} records",
        small.folded,
        big.folded
    );
    assert!(
        small.windows > 1 && big.windows > small.windows,
        "APPARATUS: {} and {} windows -- a single window measures no walk",
        small.windows,
        big.windows
    );
    assert!(
        small_corpus.wal_pieces > 1 && small_corpus.index_pieces > 1,
        "APPARATUS: the small corpus is in {} and {} pieces",
        small_corpus.wal_pieces,
        small_corpus.index_pieces
    );
    assert!(
        small.kernel_rchar.is_some() && big.kernel_rchar.is_some(),
        "APPARATUS: /proc/thread-self/io carried no rchar line"
    );
    assert!(
        small.index_bytes > 0 && big.index_bytes > 0,
        "the index-log reader claimed nothing -- before this change it claimed nothing because \
         nothing counted it, which is the defect, not the fix"
    );

    // WHAT THE TWO FIXES MOVED, at restore level.
    //
    // The index-log counter moved NO byte: every byte it now reports was already being read, and
    // reported by nothing. The whole of it is visibility. The header bound moved bytes, and this
    // is exactly how many -- a count of reads times a difference in read size, both known.
    println!(
        "what the two fixes moved, at restore level\n\
         {:>38}{:>16}{:>16}\n\
         {:>38}{:>16}{:>16}\n\
         {:>38}{:>16}{:>16}\n\
         {:>38}{:>15.2}%{:>15.2}%\n\
         {:>38}{:>16}{:>16}\n\
         {:>38}{:>15.2}%{:>15.2}%\n",
        "", format!("{SMALL} records"), format!("{BIG} records"),
        "base-header reads", small.header_reads, big.header_reads,
        "bytes the bound took off",
        small.bytes_before_the_bound() - small.named(),
        big.bytes_before_the_bound() - big.named(),
        "  as a share of what it read before",
        (small.bytes_before_the_bound() - small.named()) as f64 * 100.0
            / small.bytes_before_the_bound() as f64,
        (big.bytes_before_the_bound() - big.named()) as f64 * 100.0
            / big.bytes_before_the_bound() as f64,
        "bytes the index-log counter made VISIBLE", small.index_bytes, big.index_bytes,
        "  as a share of what this restore reads",
        small.index_bytes as f64 * 100.0 / small.named() as f64,
        big.index_bytes as f64 * 100.0 / big.named() as f64,
    );

    // The arithmetic above is exact only if every piece read was longer than the buffer that used
    // to be filled. Asserted rather than assumed.
    assert!(
        WAL_SEGMENT_BYTES >= 8 * 1024 && INDEX_SEGMENT_BYTES >= 8 * 1024,
        "APPARATUS: pieces roll at {WAL_SEGMENT_BYTES} and {INDEX_SEGMENT_BYTES} bytes, under \
         the 8 KiB fill the saving is computed against"
    );
    assert!(
        small.header_reads > 0 && big.header_reads > small.header_reads,
        "APPARATUS: {} and {} base-header reads",
        small.header_reads,
        big.header_reads
    );

    // THE CLAIM. Nothing this restore reads is unclaimed, at either size.
    for (label, cost) in [("small", &small), ("big", &big)] {
        assert_eq!(
            cost.residual(),
            Some(0),
            "{label}: {:?} bytes of this restore belong to no reader the engine names -- kernel \
             {:?}, write-ahead {} , index log {}",
            cost.residual(),
            cost.kernel_rchar,
            cost.wal_bytes,
            cost.index_bytes
        );
    }
}

/// THE RESIDUAL IS A READING, NOT AN IDENTITY.
///
/// Zero is exactly what an instrument that always answered zero would say. This runs the same
/// span with a read of a file NEITHER counter can see planted inside it, and requires the
/// residual to come back as exactly its size.
#[test]
fn the_restore_residual_recovers_a_planted_read_exactly() {
    const PLANTED: usize = 1_000_003;

    let _thresholds = Thresholds;
    let corpus = build(SMALL);

    let unseen_dir = tempfile::tempdir().unwrap();
    let unseen = unseen_dir.path().join("unseen.bin");
    std::fs::write(&unseen, vec![5u8; PLANTED]).unwrap();

    let plain = restore(&corpus, None);
    let planted = restore(&corpus, Some(&unseen));

    println!(
        "the restore residual, without and with a {PLANTED}-byte read planted inside it\n\
         {:>34}{:>14}{:>14}\n\
         {:>34}{:>14}{:>14}\n\
         {:>34}{:>14?}{:>14?}\n",
        "", "plain", "planted",
        "bytes the two readers claimed", plain.named(), planted.named(),
        "residual", plain.residual(), planted.residual(),
    );

    assert_eq!(
        (plain.replayed, planted.replayed),
        (SMALL, SMALL),
        "APPARATUS: the two restores replayed {} and {}",
        plain.replayed,
        planted.replayed
    );
    assert!(
        plain.named() > 0,
        "APPARATUS: the restore under measurement claimed no byte at all"
    );
    assert_eq!(plain.residual(), Some(0), "the plain restore left {:?}", plain.residual());
    assert_eq!(
        planted.residual(),
        Some(PLANTED as i64),
        "a {PLANTED}-byte read planted in the span came back as {:?} -- the residual is not a \
         reading",
        planted.residual()
    );
    assert_eq!(
        plain.named(),
        planted.named(),
        "the planted read must not be charged to either counter: {} against {}",
        plain.named(),
        planted.named()
    );
}
