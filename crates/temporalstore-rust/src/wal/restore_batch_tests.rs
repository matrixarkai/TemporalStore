// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;
use std::sync::mpsc;
use std::time::Duration;

fn prepare(store: &LocalWriteAheadLogStore, shard: ShardId, requests: &[(u64, u64)]) -> Result<Option<Session>, WriteAheadLogError> {
    super::prepare(store, shard, requests, true)
}

fn append(store: &LocalWriteAheadLogStore, key: &str) -> (u64, u64) {
    let record = store.append_with_sync(1, Command::StringSet {
        key: key.into(), value: format!("restore-{key}").into_bytes(),
    }, false).unwrap();
    store.flush(1).unwrap();
    let (records, truncated) = store.scan_decoded(1, 0, u64::MAX, 1 << 20).unwrap();
    assert!(!truncated);
    let log_id = records.iter().find(|(_, r)| r.sequence == record.sequence).unwrap().0;
    (log_id, record.sequence)
}

struct Roll(Option<u64>);
impl Roll {
    fn one_record() -> Self { Self(SEGMENT_BYTES_OVERRIDE.with(|slot| slot.replace(Some(1)))) }
}
impl Drop for Roll {
    fn drop(&mut self) { set_wal_segment_bytes_for_test(self.0); }
}

#[test]
fn paused_restore_read_allows_real_append_and_keeps_future_tail_absent() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, "first");
    let original = store.read_frame_at_log_id(1, first.0).unwrap();
    let future = {
        let writer = store.shard_writer(1);
        let inner = writer.lock_inner_raw();
        let end = inner.verified_len_by_shard[&1];
        let (base, header) = read_wal_base(&write_ahead_log_path(dir.path(), 1)).unwrap();
        assert!(write_ahead_log_path(dir.path(), 1).metadata().unwrap().len() > end,
            "exercise a real physical reservation beyond the logical end");
        (base + end - header, first.1 + 1)
    };
    let mut session = prepare(&store, 1, &[first, future, first]).unwrap().unwrap();
    let (sent, received) = mpsc::channel();
    let other = store.clone();
    let worker = std::thread::spawn(move || {
        let result = append(&other, "second");
        sent.send(result).unwrap();
    });
    let appended = match received.recv_timeout(Duration::from_secs(20)) {
        Ok(value) => value,
        Err(error) => {
            drop(session);
            worker.join().unwrap();
            panic!("append waited for the paused session: {error}");
        }
    };
    worker.join().unwrap();
    assert_eq!(appended, future);
    // Release the guard before joining even if an accidental writer acquisition waits.
    let writer = store.shard_writer(1);
    let held = writer.lock_inner_raw();
    let (sent, received) = mpsc::channel();
    let reader = std::thread::spawn(move || { sent.send(session.load()).unwrap(); });
    let before_release = received.recv_timeout(Duration::from_secs(20));
    drop(held);
    reader.join().unwrap();
    let frames = before_release.expect("payload must finish before releasing writer").unwrap();
    assert_eq!(frames, vec![original.clone(), None, original]);
    assert!(store.read_frame_at_log_id(1, future.0).unwrap().is_some());
}

#[test]
fn cold_restore_owns_and_initializes_checked_end_without_reading_reserved_tail() {
    let dir = tempfile::tempdir().unwrap();
    let first = {
        let writer = LocalWriteAheadLogStore::new(dir.path());
        append(&writer, "cold")
    };
    let path = write_ahead_log_path(dir.path(), 1);
    let (last, end) = last_wal_sequence_in(&path).unwrap();
    assert_eq!(last, first.1);
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(end + 4096).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let bytes = fs::read(&path).unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    assert!(store.existing_shard_writer(1).is_none());
    let mut session = prepare(&store, 1, &[first]).unwrap().unwrap();
    {
        let writer = store.shard_writer(1);
        let inner = writer.lock_inner_raw();
        assert!(inner.append_lock_by_shard.contains_key(&1));
        assert_eq!(inner.verified_len_by_shard[&1], end);
        assert_eq!(inner.last_sequence_by_shard[&1], first.1);
    }
    assert_eq!(session.load().unwrap(), store.read_frames_at_log_ids(1, &[first.0]).unwrap());
    assert_eq!(fs::read(&path).unwrap(), bytes, "valid reservation is not repaired away");
}

#[test]
fn cold_torn_tail_is_reconciled_before_the_payload_session_is_published() {
    let dir = tempfile::tempdir().unwrap();
    let (first, second, physical, expected) = {
        let writer = LocalWriteAheadLogStore::new(dir.path());
        let first = append(&writer, "survivor");
        let second = append(&writer, "torn");
        let physical = writer.resolve_log_id(1, second.0).unwrap().unwrap();
        let expected = writer.read_frame_at_log_id(1, first.0).unwrap();
        (first, second, physical, expected)
    };
    let path = write_ahead_log_path(dir.path(), 1);
    let file = OpenOptions::new().write(true).open(&path).unwrap();
    file.set_len(physical + 1).unwrap();
    file.sync_all().unwrap();
    drop(file);
    let store = LocalWriteAheadLogStore::new(dir.path());
    let mut session = prepare(&store, 1, &[first, second]).unwrap().unwrap();
    assert_eq!(session.load().unwrap(), vec![expected, None]);
    assert_eq!(last_wal_sequence_in(&path).unwrap(), (first.1, physical));
    let writer = store.shard_writer(1);
    let inner = writer.lock_inner_raw();
    assert!(inner.append_lock_by_shard.contains_key(&1));
    assert_eq!(inner.verified_len_by_shard[&1], physical);
}

#[test]
fn admitted_restore_survives_gc_and_releases_real_retention_afterward() {
    let _roll = Roll::one_record();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, "a");
    append(&store, "b");
    let last = append(&store, "c");
    let expected = store.read_frame_at_log_id(1, first.0).unwrap();
    let mut session = prepare(&store, 1, &[first]).unwrap().unwrap();
    let gc = store.gc_before_sequence_unchecked(1, last.1).unwrap();
    assert_eq!(gc.effective_retain_from_sequence, first.1);
    assert_eq!(gc.dropped_segments, 0);
    assert_eq!(session.load().unwrap(), vec![expected]);
    drop(session);
    let reclaimed = store.gc_before_sequence_unchecked(1, last.1).unwrap();
    assert!(reclaimed.dropped_segments > 0);
    assert!(store.read_frame_at_log_id(1, first.0).unwrap().is_none());
}

#[test]
fn pinned_active_descriptor_survives_a_real_prefix_rewrite() {
    let _roll = Roll(SEGMENT_BYTES_OVERRIDE.with(|slot| slot.replace(Some(0))));
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    append(&store, "discarded");
    let selected = append(&store, "selected");
    let tail = append(&store, "tail");
    let expected = store.read_frame_at_log_id(1, selected.0).unwrap();
    let mut session = prepare(&store, 1, &[selected]).unwrap().unwrap();
    let gc = store.gc_before_sequence_unchecked(1, tail.1).unwrap();
    assert_eq!(gc.effective_retain_from_sequence, selected.1);
    assert!(gc.records_removed > 0);
    assert!(store.base_offset(1).unwrap() > 0);
    assert_eq!(session.load().unwrap(), vec![expected.clone()]);
    assert_eq!(store.read_frame_at_log_id(1, selected.0).unwrap(), expected);
}

#[test]
fn cold_observer_declines_ownership_without_losing_the_existing_locked_read() {
    let dir = tempfile::tempdir().unwrap();
    let owner = LocalWriteAheadLogStore::new(dir.path());
    let selected = append(&owner, "owned");
    let expected = owner.read_frames_at_log_ids(1, &[selected.0]).unwrap();
    let observer = LocalWriteAheadLogStore::new(dir.path());
    assert!(prepare(&observer, 1, &[selected]).is_err());
    assert_eq!(observer.block_retention_floor(1), None);
    assert_eq!(observer.read_frames_at_log_ids(1, &[selected.0]).unwrap(), expected);
}

struct ResetHook;
impl Drop for ResetHook {
    fn drop(&mut self) { GC_PHASE_HOOK.with(|hook| { hook.borrow_mut().take(); }); }
}

#[test]
fn already_planned_gc_refuses_restore_admission_before_descriptor_capture() {
    let _roll = Roll::one_record();
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, "a");
    append(&store, "b");
    let last = append(&store, "c");
    let survivor = store.read_frame_at_log_id(1, last.0).unwrap();
    let (sent, ready) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    let other = store.clone();
    let worker = std::thread::spawn(move || {
        let _reset = ResetHook;
        GC_PHASE_HOOK.with(|hook| {
            *hook.borrow_mut() = Some(Box::new(move |phase| {
                if phase == "planned" {
                    sent.send(()).unwrap();
                    resumed.recv_timeout(Duration::from_secs(20)).unwrap();
                }
            }));
        });
        other.gc_before_sequence_unchecked(1, last.1)
    });
    let reached = ready.recv_timeout(Duration::from_secs(20));
    let refused = reached.is_ok() && prepare(&store, 1, &[first]).unwrap().is_none();
    let _ = resume.send(());
    let gc = worker.join().unwrap().unwrap();
    assert!(reached.is_ok());
    assert!(refused);
    assert!(gc.dropped_segments > 0, "the refusal must be against a productive real plan");
    assert_eq!(store.read_frame_at_log_id(1, last.0).unwrap(), survivor);
}

#[test]
fn truncated_captured_extent_and_wrong_sequence_are_not_returned_as_valid_frames() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let first = append(&store, "integrity");
    let mut wrong = prepare(&store, 1, &[(first.0, first.1 + 1)]).unwrap().unwrap();
    assert!(matches!(wrong.load(), Err(WriteAheadLogError::Corruption(_))));
    drop(wrong);
    let mut captured = prepare(&store, 1, &[first]).unwrap().unwrap();
    let file = OpenOptions::new().write(true).open(write_ahead_log_path(dir.path(), 1)).unwrap();
    file.set_len(0).unwrap();
    file.sync_all().unwrap();
    assert!(captured.load().is_err());
}

#[test]
fn empty_zero_sequence_and_oversized_requests_decline_without_invented_floor() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    assert!(prepare(&store, 1, &[]).unwrap().is_none());
    assert!(prepare(&store, 1, &[(0, 0)]).unwrap().is_none());
    assert!(prepare(&store, 1, &vec![(0, 1); 4097]).unwrap().is_none());
    assert!(store.existing_shard_writer(1).is_none());
}

#[test]
fn observer_intent_declines_before_owner_or_lease_creation() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    assert!(super::prepare(&store, 1, &[(0, 1)], false).unwrap().is_none());
    assert!(store.existing_shard_writer(1).is_none());
    assert!(!dir.path().join("shard-1.wal.lock").exists());
}

#[test]
fn a_real_frame_crc_byte_flip_is_rejected_by_the_admitted_payload_reader() {
    let dir = tempfile::tempdir().unwrap();
    let store = LocalWriteAheadLogStore::new(dir.path());
    let selected = append(&store, "crc-integrity");
    let frame = store.read_frame_at_log_id(1, selected.0).unwrap().unwrap();
    assert_eq!(frame[0], crate::log_framing::FRAME_MAGIC_V3);
    let mut at = 1;
    loop {
        let byte = frame[at];
        at += 1;
        assert!(at <= 11);
        if byte & 0x80 == 0 { break; }
    }
    let payload = crate::log_framing::decode_line(&frame).unwrap().to_vec();
    assert_eq!(u32::from_le_bytes(frame[at..at + 4].try_into().unwrap()), crate::checksum::crc32c(&payload));
    let physical = store.resolve_log_id(1, selected.0).unwrap().unwrap();
    let mut session = prepare(&store, 1, &[selected]).unwrap().unwrap();
    assert!(session.spans.iter().all(|span| !span.sealed));
    let path = write_ahead_log_path(dir.path(), 1);
    let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
    let mut disk_frame = vec![0; frame.len()];
    file.seek(SeekFrom::Start(physical)).unwrap();
    file.read_exact(&mut disk_frame).unwrap();
    assert_eq!(disk_frame, frame, "mutate the actual selected frame, not a copied decoder input");
    let mut corrupt = frame.clone();
    corrupt[at] ^= 1;
    assert_eq!(&corrupt[at + 4..], &frame[at + 4..], "payload bytes remain exact");
    file.seek(SeekFrom::Start(physical + at as u64)).unwrap();
    file.write_all(&[corrupt[at]]).unwrap();
    file.sync_all().unwrap();
    assert!(crate::log_framing::decode_line(&corrupt).is_err());
    assert!(session.load().is_err(), "an active same-inode same-length file must still verify its CRC");
    drop(session);
    assert_eq!(store.block_retention_floor(1), None);
}
