// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The delta records of concurrent writers reach the index log as one write per batch.
//!
//! # WHY THIS EXISTS
//!
//! Every set ran, under the engine's write guard, an `open`, a probe of the active piece, an
//! encode, a framing with a digest, a `write` and a `close` of this log, for a record nothing waits
//! on: under the single-barrier default the ack is the WAL barrier, and a delta tail lost in a
//! crash is rebuilt by WAL replay. Measured at 16 writers on the 100k-key fixture: 0.7-0.9 index
//! log writes per set, never batched, while the WAL beside it writes once per batch.
//!
//! A record nothing waits on is now numbered and queued, and the whole batch is written once. What
//! these tests pin is what makes that safe:
//!
//!   1. a record is QUEUED, not written, and it is numbered at once (the caller stamps the
//!      sequence into its bucket before anything is written);
//!   2. anything that reads the log finishes the queue first, in one write, in order;
//!   3. a record that asks to be durable is written after what is queued, so the log stays in
//!      sequence order;
//!   4. the last handle going away writes the tail, and the next store continues the numbering;
//!   5. a failed batch is written again whole, before anything newer, and never twice;
//!   6. many threads share writes and no record is lost, duplicated or reordered;
//!   7. with the switch off, a record is on disk when its append returns, as it always was;
//!   8. a queue nobody reads is written by the loop flusher.
//!
//! Test builds default the batched write OFF on the thread, so the tests written against
//! write-at-once keep meaning what they say; these turn it on.

use super::*;

fn item(bucket: u32, key: &str) -> IndexItem {
    IndexItem {
        kind: IndexItemKind::Block,
        routing_bucket: bucket,
        block_ref_key: key.to_string(),
        object_key: key.into(),
        model_id: "m".to_string(),
        object_id: 1,
        entry: None,
        deleted: false,
    }
}

/// A delta record nothing waits on, the way the engine's single-command path appends one.
fn append(store: &LocalIndexLogStore, shard: ShardId, key: &str, wal: u64) -> u64 {
    store
        .append_delta(shard, vec![item(1, key)], Vec::new(), Some(wal), None, false, false)
        .expect("the append is accepted")
}

fn queued(store: &LocalIndexLogStore, shard: ShardId) -> usize {
    store.stream(shard).lock().unwrap().queued.len()
}

fn stuck(store: &LocalIndexLogStore, shard: ShardId) -> usize {
    store.stream(shard).lock().unwrap().stuck.len()
}

/// Physical writes made so far, read WITHOUT finishing the queue.
fn writes(store: &LocalIndexLogStore) -> u64 {
    store.aggregate_stats().writes
}

fn piece_len(dir: &Path, shard: ShardId) -> u64 {
    active_index_log(dir, shard).1
}

/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_record_nothing_waits_on_is_numbered_and_queued_not_written() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    let sequences: Vec<u64> = (0..40).map(|i| append(&store, 5, &format!("k{i}"), i + 1)).collect();
    assert_eq!((1..=40).collect::<Vec<u64>>(), sequences, "numbered at once, in order");
    assert_eq!(40, queued(&store, 5), "denominator: all forty are waiting");
    assert_eq!(0, writes(&store), "nothing has been written");
    assert_eq!(0, piece_len(dir.path(), 5), "and the piece is empty on disk");
}

/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_reader_finishes_the_queue_first_and_it_is_one_write_in_order() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for i in 0..40u64 {
        append(&store, 5, &format!("k{i}"), i + 1);
    }
    let records = store.read_delta_records(5, 0).expect("the log reads");
    assert_eq!(40, records.len(), "every record the appends returned a number for");
    let sequences: Vec<u64> = records.iter().map(|record| record.sequence).collect();
    assert_eq!((1..=40).collect::<Vec<u64>>(), sequences, "in the order they were numbered");
    let anchors: Vec<Option<u64>> = records.iter().map(|record| record.applied_wal_sequence).collect();
    assert_eq!((1..=40).map(Some).collect::<Vec<_>>(), anchors, "each carries its own anchor");
    assert_eq!(1, writes(&store), "forty records, ONE write");
    assert_eq!(0, queued(&store, 5));
    assert!(piece_len(dir.path(), 5) > 0);
}

/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_record_that_asks_to_be_durable_is_written_after_what_is_queued() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for i in 0..3u64 {
        append(&store, 7, &format!("q{i}"), i + 1);
    }
    let durable = store
        .append_delta(7, vec![item(1, "now")], Vec::new(), Some(4), None, false, true)
        .expect("the durable append is written");
    assert_eq!(4, durable, "numbered after the three that were queued");
    assert_eq!(0, queued(&store, 7), "the queue went first");
    let sequences: Vec<u64> = store
        .read_delta_records(7, 0)
        .unwrap()
        .iter()
        .map(|record| record.sequence)
        .collect();
    assert_eq!(vec![1, 2, 3, 4], sequences, "the log is in sequence order");
}

/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn dropping_the_store_writes_the_tail_and_the_next_store_continues_the_numbering() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    {
        let store = LocalIndexLogStore::new(dir.path());
        for i in 0..5u64 {
            append(&store, 3, &format!("k{i}"), i + 1);
        }
        assert_eq!(5, queued(&store, 3), "denominator: nothing was written before the drop");
    }
    assert!(piece_len(dir.path(), 3) > 0, "the drop wrote the tail");
    let reopened = LocalIndexLogStore::new(dir.path());
    assert_eq!(5, reopened.read_delta_records(3, 0).unwrap().len());
    assert_eq!(6, append(&reopened, 3, "next", 6), "the next record is number six");
}

/// A failed batch is written again, whole, before anything newer, and never twice.
///
/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_failed_batch_is_retried_whole_and_never_duplicated() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for i in 0..5u64 {
        append(&store, 9, &format!("k{i}"), i + 1);
    }
    FAIL_NEXT_FRAME_WRITE.with(|cell| cell.set(true));
    assert_eq!(0, store.log_len_bytes(9), "the write failed, so the piece is still empty");
    assert_eq!(1, stuck(&store, 9), "the batch is held, already encoded");
    assert_eq!(0, queued(&store, 9));
    // a newer record queued behind the held batch
    assert_eq!(6, append(&store, 9, "later", 6));
    let sequences: Vec<u64> = store
        .read_delta_records(9, 0)
        .unwrap()
        .iter()
        .map(|record| record.sequence)
        .collect();
    assert_eq!(vec![1, 2, 3, 4, 5, 6], sequences, "the held batch first, then the newer record, each once");
    assert_eq!(0, stuck(&store, 9));
}

/// A write that stops part-way is cut back, so the retry leaves no torn record in front of a whole one.
///
/// The injected failure writes the first 40 bytes of the batch and then fails: with the piece NOT
/// cut back, the retry would append the whole batch after those 40 bytes, and the log would hold a
/// torn frame followed by every record, which a reader refuses.
///
/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_write_that_stops_part_way_is_cut_back_so_the_retry_leaves_no_torn_record() {
    let _on = group_write_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for i in 0..5u64 {
        append(&store, 6, &format!("k{i}"), i + 1);
    }
    FAIL_PARTIAL_FRAME_WRITE.with(|cell| cell.set(Some(40)));
    assert_eq!(0, store.log_len_bytes(6), "the piece was cut back to where the batch started");
    assert_eq!(1, stuck(&store, 6));
    let records = store.read_delta_records(6, 0).expect("the retried log reads whole");
    let sequences: Vec<u64> = records.iter().map(|record| record.sequence).collect();
    assert_eq!(vec![1, 2, 3, 4, 5], sequences, "each record once, none torn");
}

/// Many threads: no record is lost, duplicated or reordered, and they share writes.
///
/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn appenders_on_many_threads_share_writes_and_no_record_is_lost_or_reordered() {
    const THREADS: usize = 8;
    const PER: usize = 125;
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    let mut handles = Vec::new();
    for t in 0..THREADS {
        let store = store.clone();
        handles.push(std::thread::spawn(move || {
            // the switch is per thread in test builds
            let _on = group_write_in_test(true);
            (0..PER)
                .map(|i| append(&store, 1, &format!("t{t}-{i}"), (t * PER + i + 1) as u64))
                .collect::<Vec<u64>>()
        }));
    }
    let mut all: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    all.sort_unstable();
    let total = (THREADS * PER) as u64;
    assert_eq!((1..=total).collect::<Vec<u64>>(), all, "every number handed out exactly once");

    let records = store.read_delta_records(1, 0).unwrap();
    assert_eq!(THREADS * PER, records.len(), "every record is in the log");
    let sequences: Vec<u64> = records.iter().map(|record| record.sequence).collect();
    assert_eq!((1..=total).collect::<Vec<u64>>(), sequences, "in sequence order on disk");
    let keys: std::collections::BTreeSet<String> =
        records.iter().map(|record| record.items[0].block_ref_key.clone()).collect();
    assert_eq!(THREADS * PER, keys.len(), "no record twice");
    assert!(
        writes(&store) < total,
        "{} records took {} writes: they were not batched",
        total,
        writes(&store)
    );
}

/// With the switch off, a record is on disk when its append returns.
///
/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn with_the_switch_off_every_record_is_written_at_once() {
    let _off = group_write_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    assert_eq!(1, append(&store, 2, "a", 1));
    assert_eq!(0, queued(&store, 2));
    assert_eq!(1, writes(&store));
    assert!(piece_len(dir.path(), 2) > 0, "on disk before the call returned");
    assert_eq!(2, append(&store, 2, "b", 2));
    assert_eq!(2, writes(&store), "one write per record, as it always was");
}

/// A queue nobody reads is written by the loop flusher thread.
///
/// Waits on a CONDITION, not a duration: the poll ends the moment the queue is empty, and the
/// deadline only bounds a failure.
///
/// rust-internal: exercises the index log's write path, no product behaviour
#[test]
fn a_queue_nobody_reads_is_written_by_the_loop_flusher() {
    let _on = group_write_in_test(true);
    let _flusher = flusher_in_test(true);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for i in 0..10u64 {
        append(&store, 4, &format!("k{i}"), i + 1);
    }
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    while queued(&store, 4) > 0 && std::time::Instant::now() < deadline {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(0, queued(&store, 4), "the flusher wrote the queue without being asked");
    assert!(piece_len(dir.path(), 4) > 0);
    assert!(writes(&store) >= 1);
    assert_eq!(10, store.read_delta_records(4, 0).unwrap().len());
}

fn store_with_pending_limit(path: &Path, limit: usize) -> LocalIndexLogStore {
    let mut store = LocalIndexLogStore::new(path);
    Arc::get_mut(&mut store.root).unwrap().pending_limit = limit;
    store
}

fn pending_count(root: &IndexLogRoot) -> usize {
    root.pending_records.load(AtomicOrdering::Relaxed)
}

#[test]
fn pending_budget_spans_failed_streams_and_an_in_flight_batch() {
    use std::sync::mpsc;
    use std::time::Duration;

    let _group = group_write_in_test(true);
    let _inactive = flusher_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let store = store_with_pending_limit(dir.path(), 6);
    for shard in 1..=3 {
        assert_eq!(append(&store, shard, "first", 1), 1);
        assert_eq!(append(&store, shard, "second", 2), 2);
    }
    for shard in 1..=2 {
        FAIL_PARTIAL_FRAME_WRITE.with(|fault| fault.set(Some(40)));
        assert!(store.capture_checkpoint_relocations(shard, 0, |_| {}).is_err());
        assert_eq!(piece_len(dir.path(), shard), 0);
        assert_eq!(stuck(&store, shard), 1);
        assert_eq!(pending_count(&store.root), 6);
    }

    let (arrive, parked) = mpsc::channel();
    let (resume, proceed) = mpsc::channel();
    let writing = store.clone();
    let worker = std::thread::spawn(move || {
        BEFORE_PENDING_INDEX_ENCODE.with(|slot| *slot.borrow_mut() = Some((arrive, proceed)));
        FAIL_PARTIAL_FRAME_WRITE.with(|fault| fault.set(Some(40)));
        writing.capture_checkpoint_relocations(3, 0, |_| {})
    });
    let at_batch = parked.recv_timeout(Duration::from_secs(5));
    let rejected = store.append_delta(4, vec![item(1, "peer")], Vec::new(), Some(1), None, false, false);
    let charged_while_encoding = pending_count(&store.root);
    let released = resume.send(());
    let failed = worker.join().unwrap();
    at_batch.unwrap();
    released.unwrap();
    assert!(rejected.is_err());
    assert_eq!(charged_while_encoding, 6);
    assert!(failed.is_err());
    assert_eq!(stuck(&store, 3), 1);
    assert_eq!(pending_count(&store.root), 6);
    assert!(store.append_delta(4, vec![item(1, "peer")], Vec::new(), Some(1), None, false, false).is_err());

    let sequences = |store: &LocalIndexLogStore, shard| {
        store.read_delta_records(shard, 0).unwrap().into_iter().map(|row| row.sequence)
            .collect::<Vec<_>>()
    };
    assert_eq!(sequences(&store, 1), [1, 2]);
    assert_eq!(pending_count(&store.root), 4);
    assert_eq!(append(&store, 4, "peer", 1), 1);
    assert_eq!(append(&store, 2, "third", 3), 3);
    FAIL_NEXT_FRAME_WRITE.with(|fault| fault.set(true));
    assert!(store.capture_checkpoint_relocations(2, 0, |_| {}).is_err());
    assert_eq!(queued(&store, 2), 1);
    assert_eq!(pending_count(&store.root), 6);
    assert_eq!(sequences(&store, 2), [1, 2, 3]);
    assert_eq!(pending_count(&store.root), 3);
    assert_eq!(sequences(&store, 3), [1, 2]);
    assert_eq!(sequences(&store, 4), [1]);
    assert_eq!(pending_count(&store.root), 0);
    drop(store);
    let reopened = LocalIndexLogStore::new(dir.path());
    for (shard, expected) in [(1, &[1, 2][..]), (2, &[1, 2, 3]), (3, &[1, 2]), (4, &[1])] {
        assert_eq!(sequences(&reopened, shard).as_slice(), expected);
    }
}

#[test]
fn pending_encode_discard_and_terminal_drop_release_exact_charges() {
    let _group = group_write_in_test(true);
    let _inactive = flusher_in_test(false);
    let dir = tempfile::tempdir().unwrap();
    let store = store_with_pending_limit(dir.path(), 2);
    let root = Arc::clone(&store.root);
    assert_eq!(append(&store, 1, "unencodable", 1), 1);
    assert_eq!(append(&store, 1, "retry", 2), 2);
    FAIL_PENDING_INDEX_ENCODE.with(|fault| fault.set(Some(1)));
    FAIL_PARTIAL_FRAME_WRITE.with(|fault| fault.set(Some(40)));
    assert!(matches!(store.capture_checkpoint_relocations(1, 0, |_| {}), Err(IndexLogError::Encoding(_))));
    assert_eq!(pending_count(&root), 1, "only the encoded retry remains charged");
    assert_eq!(stuck(&store, 1), 1);
    assert_eq!(piece_len(dir.path(), 1), 0);
    assert_eq!(append(&store, 1, "behind retry", 3), 3);
    assert_eq!(pending_count(&root), 2);
    FAIL_NEXT_FRAME_WRITE.with(|fault| fault.set(true));
    drop(store);
    assert_eq!(pending_count(&root), 0, "terminal drop released both the retry and the fresh queue");
    let reopened = LocalIndexLogStore::new(dir.path());
    assert!(reopened.read_delta_records(1, 0).unwrap().is_empty());
}
