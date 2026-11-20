// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

const SHARD: ShardId = 71;

struct Rolling;
impl Rolling {
    fn at(bytes: u64) -> Self {
        set_index_log_segment_bytes_for_test(Some(bytes));
        Self
    }
}
impl Drop for Rolling {
    fn drop(&mut self) {
        set_index_log_segment_bytes_for_test(None);
    }
}

fn append(store: &LocalIndexLogStore, item: usize) {
    store
        .append_delta(
            SHARD,
            vec![IndexItem {
                kind: IndexItemKind::Block,
                routing_bucket: 0,
                block_ref_key: item.to_string(),
                object_key: format!("key-{item}-{}", "v".repeat(item % 31)).into(),
                model_id: "string".into(),
                object_id: item as u64,
                deleted: false,
                entry: None,
            }],
            Vec::new(),
            Some(if item % 2 == 0 { 1 } else { 9 }),
            None,
            false,
            false,
        )
        .unwrap();
}

fn values(summary: IndexLogGateSummary) -> (u64, usize, usize, u64, usize, usize) {
    (
        summary.bytes,
        summary.records,
        summary.removable_records,
        summary.removable_bytes,
        summary.pieces,
        summary.pieces_named,
    )
}

fn measured(store: &LocalIndexLogStore, floor: u64) -> (IndexLogGateSummary, (u64, u64)) {
    let before = index_log_piece_read_counts_on_this_thread();
    let summary = store.gate_summary(SHARD, floor);
    let after = index_log_piece_read_counts_on_this_thread();
    (summary, (after.0 - before.0, after.1 - before.1))
}

#[test]
fn append_maintained_gate_matches_cold_reads_at_every_floor() {
    let _rolling = Rolling::at(0);
    for count in [32, 512] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for item in 0..count {
            append(&store, item);
        }
        for floor in [0, 1, 17, count as u64 / 2, count as u64 + 1, u64::MAX] {
            let cold = LocalIndexLogStore::new(dir.path());
            let (control, control_reads) = measured(&cold, floor);
            assert!(control_reads.0 > 0 && control.records > 0);
            let (actual, reads) = measured(&store, floor);
            assert_eq!(values(actual), values(control));
            assert_eq!(reads, (0, 0), "a warm gate must not read active records");
        }
    }
}

#[test]
fn a_cold_gate_reads_once_and_later_appends_extend_it() {
    let _rolling = Rolling::at(0);
    let dir = tempfile::tempdir().unwrap();
    let writer = LocalIndexLogStore::new(dir.path());
    for item in 0..32 {
        append(&writer, item);
    }
    drop(writer);
    let store = LocalIndexLogStore::new(dir.path());
    let (first, reads) = measured(&store, 17);
    assert!(reads.0 > 0);
    let (again, reads) = measured(&store, 17);
    assert_eq!(values(again), values(first));
    assert_eq!(reads, (0, 0));
    append(&store, 32);
    let expected = LocalIndexLogStore::new(dir.path()).gate_summary(SHARD, u64::MAX);
    let (actual, reads) = measured(&store, u64::MAX);
    assert_eq!(values(actual), values(expected));
    assert_eq!(actual.records, 33);
    assert_eq!(reads, (0, 0));
}

#[test]
fn collection_rewrites_and_sequence_holes_invalidate_the_gate() {
    let _rolling = Rolling::at(0);
    for reflected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for item in 0..12 {
            append(&store, item);
        }
        let before = store.gate_summary(SHARD, u64::MAX);
        if reflected {
            store.gc_reflected_before_anchor(SHARD, 1, 13, 0).unwrap();
        } else {
            store.gc_before_sequence_limited(SHARD, 7, 2).unwrap();
        }
        assert!(!store
            .inner
            .lock()
            .unwrap()
            .active_gate_records_by_shard
            .contains_key(&SHARD));
        let expected = LocalIndexLogStore::new(dir.path()).gate_summary(SHARD, 7);
        assert!(expected.records > 0 && expected.records < before.records);
        let (actual, reads) = measured(&store, 7);
        assert!(reads.0 > 0, "a rewritten piece must be read again");
        assert_eq!(values(actual), values(expected));
        let (again, reads) = measured(&store, 7);
        assert_eq!(values(again), values(expected));
        assert_eq!(reads, (0, 0));
        append(&store, 20);
        let expected = LocalIndexLogStore::new(dir.path()).gate_summary(SHARD, 11);
        let (actual, reads) = measured(&store, 11);
        assert_eq!(values(actual), values(expected));
        assert_eq!(reads, (0, 0));
    }
}

#[test]
fn rolling_replaces_only_the_active_gate_records() {
    let _rolling = Rolling::at(512);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for item in 0..96 {
        append(&store, item);
    }
    assert!(store.piece_count(SHARD) > 2);
    for floor in [0, 3, 27, 96, 97] {
        let (expected, control_reads) = measured(&LocalIndexLogStore::new(dir.path()), floor);
        assert!(control_reads.0 > 0);
        let (actual, reads) = measured(&store, floor);
        assert_eq!(values(actual), values(expected));
        assert_eq!(reads, (0, 0));
    }
}

#[test]
fn active_gate_memory_is_capped_and_a_roll_starts_a_new_cache() {
    let _rolling = Rolling::at(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for item in 0..MAX_ACTIVE_GATE_RECORDS {
        append(&store, item);
    }
    {
        let inner = store.inner.lock().unwrap();
        let cached = inner.active_gate_records_by_shard.get(&SHARD).unwrap();
        assert_eq!(cached.records.len(), MAX_ACTIVE_GATE_RECORDS);
        assert!(cached.records.capacity() * std::mem::size_of::<(u64, u64)>() <= 64 * 1024);
    }
    append(&store, MAX_ACTIVE_GATE_RECORDS);
    for _ in 0..2 {
        let (summary, reads) = measured(&store, u64::MAX);
        assert_eq!(summary.records, MAX_ACTIVE_GATE_RECORDS + 1);
        assert!(reads.0 > 0);
        assert!(!store
            .inner
            .lock()
            .unwrap()
            .active_gate_records_by_shard
            .contains_key(&SHARD));
    }
    set_index_log_segment_bytes_for_test(Some(512));
    append(&store, MAX_ACTIVE_GATE_RECORDS + 1);
    let (summary, reads) = measured(&store, u64::MAX);
    assert_eq!(summary.records, MAX_ACTIVE_GATE_RECORDS + 2);
    assert_eq!(reads, (0, 0));
    assert_eq!(
        store.inner.lock().unwrap().active_gate_records_by_shard[&SHARD]
            .records
            .len(),
        1
    );
}

#[test]
fn anchor_and_delta_appends_share_the_same_gate_boundaries() {
    let _rolling = Rolling::at(0);
    let dir = tempfile::tempdir().unwrap();
    let store = LocalIndexLogStore::new(dir.path());
    for item in 0..12 {
        if item % 3 == 0 {
            store
                .append_index_bytes(SHARD, format!("TSIDX\x01\x02{item}").as_bytes())
                .unwrap();
        } else {
            append(&store, item);
        }
    }
    for floor in [0, 2, 8, 13] {
        let expected = LocalIndexLogStore::new(dir.path()).gate_summary(SHARD, floor);
        let (actual, reads) = measured(&store, floor);
        assert_eq!(values(actual), values(expected));
        assert_eq!(reads, (0, 0));
    }
}

#[test]
fn undecodable_and_nonmonotonic_records_never_create_a_gate_cache() {
    let _rolling = Rolling::at(0);
    for nonmonotonic in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for item in 0..8 {
            append(&store, item);
        }
        let payload = if nonmonotonic {
            encode_index_payload(
                &IndexLogRecord {
                    shard_id: SHARD,
                    sequence: 1,
                    index: serde_json::json!({"kept": true}),
                },
                INDEX_LOG_SHAPE_WHOLE,
            )
            .unwrap()
        } else {
            b"not an index record".to_vec()
        };
        let path = index_log_path(dir.path(), SHARD);
        OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(&crate::log_framing::encode_record(&payload))
            .unwrap();
        for _ in 0..2 {
            let (summary, reads) = measured(&store, 4);
            assert_eq!(summary.records, 9);
            assert_eq!(summary.removable_records, if nonmonotonic { 4 } else { 3 });
            assert!(reads.0 > 0);
            assert!(!store
                .inner
                .lock()
                .unwrap()
                .active_gate_records_by_shard
                .contains_key(&SHARD));
        }
    }
}

#[test]
fn a_torn_active_tail_is_never_cached_as_a_complete_piece() {
    let _rolling = Rolling::at(0);
    let dir = tempfile::tempdir().unwrap();
    let writer = LocalIndexLogStore::new(dir.path());
    for item in 0..8 {
        append(&writer, item);
    }
    drop(writer);
    let path = index_log_path(dir.path(), SHARD);
    let file = OpenOptions::new().write(true).open(path).unwrap();
    file.set_len(file.metadata().unwrap().len() - 3).unwrap();
    drop(file);
    let store = LocalIndexLogStore::new(dir.path());
    for _ in 0..2 {
        let (summary, reads) = measured(&store, u64::MAX);
        assert_eq!(summary.records, 7);
        assert_eq!(summary.removable_records, 7);
        assert!(reads.0 > 0);
        assert!(!store
            .inner
            .lock()
            .unwrap()
            .active_gate_records_by_shard
            .contains_key(&SHARD));
    }
}

fn collect(store: &LocalIndexLogStore, reflected: bool, count: usize) -> IndexLogGcReport {
    if reflected {
        store
            .gc_reflected_before_anchor(SHARD, 1, count as u64 + 1, 0)
            .unwrap()
    } else {
        store
            .gc_before_sequence_limited(SHARD, count as u64 / 2 + 1, 4)
            .unwrap()
    }
}

#[test]
fn warm_collection_validates_the_active_piece_only_once() {
    let _rolling = Rolling::at(0);
    for reflected in [false, true] {
        for count in [32, 512] {
            let dir = tempfile::tempdir().unwrap();
            let store = LocalIndexLogStore::new(dir.path());
            for item in 0..count {
                append(&store, item);
            }
            let bytes = store.log_len_bytes(SHARD);
            let before = index_log_piece_read_counts_on_this_thread();
            let report = collect(&store, reflected, count);
            let after = index_log_piece_read_counts_on_this_thread();
            assert!(report.records_removed > 0);
            assert_eq!(
                after.0 - before.0,
                bytes,
                "the collector must validate records, not read the same tail twice"
            );
        }
    }
}

#[test]
fn a_validated_cold_gate_also_proves_the_collection_tail() {
    let _rolling = Rolling::at(0);
    let dir = tempfile::tempdir().unwrap();
    let writer = LocalIndexLogStore::new(dir.path());
    for item in 0..32 {
        append(&writer, item);
    }
    drop(writer);
    let store = LocalIndexLogStore::new(dir.path());
    let gate = store.gate_summary(SHARD, 17);
    assert_eq!(gate.records, 32);
    assert!(!store
        .inner
        .lock()
        .unwrap()
        .last_sequence_by_shard
        .contains_key(&SHARD));
    let before = index_log_piece_read_counts_on_this_thread();
    let report = collect(&store, false, 32);
    let after = index_log_piece_read_counts_on_this_thread();
    assert_eq!(report.records_removed, 4);
    assert_eq!(after.0 - before.0, gate.bytes);
}

#[test]
fn cold_collection_keeps_the_repair_pass() {
    let _rolling = Rolling::at(0);
    for reflected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let writer = LocalIndexLogStore::new(dir.path());
        for item in 0..32 {
            append(&writer, item);
        }
        drop(writer);
        let store = LocalIndexLogStore::new(dir.path());
        let bytes = store.log_len_bytes(SHARD);
        let before = index_log_piece_read_counts_on_this_thread();
        let report = collect(&store, reflected, 32);
        let after = index_log_piece_read_counts_on_this_thread();
        assert!(report.records_removed > 0);
        assert_eq!(after.0 - before.0, bytes * 2);
    }
}

#[test]
fn a_changed_warm_tail_is_repaired_before_collection() {
    let _rolling = Rolling::at(0);
    for reflected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for item in 0..32 {
            append(&store, item);
        }
        let record = IndexLogRecord {
            shard_id: SHARD,
            sequence: 33,
            index: serde_json::json!({"torn": true}),
        };
        let payload = encode_index_payload(&record, INDEX_LOG_SHAPE_WHOLE).unwrap();
        let frame = crate::log_framing::encode_record(&payload);
        OpenOptions::new()
            .append(true)
            .open(index_log_path(dir.path(), SHARD))
            .unwrap()
            .write_all(&frame[..frame.len() - 3])
            .unwrap();
        let report = collect(&store, reflected, 32);
        assert!(report.records_removed > 0);
        append(&store, 40);
        assert_eq!(
            store
                .read_delta_records(SHARD, 0)
                .unwrap()
                .last()
                .unwrap()
                .sequence,
            33
        );
    }
}

#[test]
fn warm_collection_still_refuses_complete_corrupt_records() {
    let _rolling = Rolling::at(0);
    for reflected in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let store = LocalIndexLogStore::new(dir.path());
        for item in 0..32 {
            append(&store, item);
        }
        let path = index_log_path(dir.path(), SHARD);
        let mut bytes = fs::read(&path).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&path, &bytes).unwrap();
        let result = if reflected {
            store.gc_reflected_before_anchor(SHARD, 1, 33, 0)
        } else {
            store.gc_before_sequence_limited(SHARD, 17, 4)
        };
        assert!(
            result.is_err(),
            "cached metadata must not replace record validation"
        );
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}
