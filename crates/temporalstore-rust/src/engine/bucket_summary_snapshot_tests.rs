// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

use super::*;

fn open(dir: &std::path::Path, end: u32) -> TemporalEngine {
    let engine = TemporalEngine::with_local_dirs(
        16 << 20,
        dir.join("cache"),
        dir.join("blocks"),
        dir.join("indexes"),
    );
    let loaded = engine.load_shard_with(LoadShardRequest {
        shard_id: 1,
        load_version: 0,
        local_node_id: None,
        shard_uri: String::new(),
        start_routing_bucket: 0,
        end_routing_bucket: end,
        readonly: false,
        table_name: String::new(),
    });
    assert!(loaded.status.ok, "{:?}", loaded.status);
    let response = engine.batch_execute(crate::types::BatchExecuteRequest {
        shard_id: 1,
        commands: (0..256)
            .map(|index| Command::StringSet {
                key: format!("snapshot-{index:04}"),
                value: vec![b'x'; 128],
            })
            .collect(),
    });
    assert!(response.status.ok);
    assert!(response.responses.iter().all(|row| row.status.ok));
    engine
}

fn write(engine: &TemporalEngine) {
    let response = engine.execute(ExecuteRequest {
        shard_id: 1,
        command: Command::StringSet {
            key: "snapshot-0000".into(),
            value: vec![b'y'; 128],
        },
    });
    assert!(response.status.ok, "{:?}", response.status);
}

fn locked_result(
    engine: &TemporalEngine,
    end: u32,
    with_digests: bool,
) -> (
    Vec<BucketStorageSummary>,
    BTreeMap<u32, BucketFingerprintDigest>,
) {
    let shards = engine.shards.read().unwrap();
    let (summaries, digests) = bucket_summaries_and_digests(1, &shards[&1], 0, end, with_digests);
    let summaries = match latest_bucket_dump_manifest_shared_at(&engine.index_dir, 1) {
        Some(manifest) => merge_last_dump_sequence(summaries, &manifest),
        None => summaries,
    };
    (summaries, digests)
}

#[test]
fn bucket_summary_snapshot_matches_the_locked_result_in_both_modes() {
    for end in [127, u32::MAX] {
        let dir = tempfile::tempdir().unwrap();
        let engine = open(dir.path(), end);
        for command in [
            Command::HashMultiSet {
                key: "catalog".into(),
                entries: (0..32)
                    .map(|index| (format!("field-{index}"), vec![b'h'; 128]))
                    .collect(),
            },
            Command::SetAdd {
                key: "order".into(),
                member: b"member".to_vec(),
            },
            Command::StringDelete {
                key: "snapshot-0001".into(),
            },
        ] {
            assert!(
                engine
                    .execute(ExecuteRequest {
                        shard_id: 1,
                        command
                    })
                    .status
                    .ok
            );
        }
        for with_digests in [false, true] {
            let expected = locked_result(&engine, end, with_digests);
            assert_eq!(
                engine.bucket_storage_summaries_with_digests(1, with_digests),
                expected
            );
            assert_eq!(
                engine.bucket_storage_summaries_with_digests(999, with_digests),
                Default::default()
            );
        }
    }
}

#[test]
fn bucket_summary_snapshot_allows_writer_progress_before_digest_completion() {
    fn assert_send<T: Send>() {}
    assert_send::<BucketSummarySnapshot>();
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 127);
    let expected = locked_result(&engine, 127, true);
    let snapshot = engine.capture_bucket_storage_summaries(1, true).unwrap();
    assert!(
        engine.shards.try_write().is_ok(),
        "capture retained a shard guard"
    );
    let (sent, received) = std::sync::mpsc::sync_channel(1);
    let writer = engine.clone();
    let thread = std::thread::spawn(move || {
        write(&writer);
        sent.send(()).unwrap();
    });
    // A deadlock guard, not a latency target: the fingerprint phase is deliberately deferred.
    received
        .recv_timeout(std::time::Duration::from_secs(5))
        .unwrap();
    thread.join().unwrap();
    assert_eq!(snapshot.finish(), expected);
    assert_ne!(
        engine.bucket_storage_summaries_with_digests(1, true).1,
        expected.1
    );
}

#[test]
fn bucket_summary_snapshot_preserves_manifest_annotations_without_pinning_the_image() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 127);
    let buckets = engine
        .bucket_storage_summaries(1)
        .iter()
        .map(|row| row.routing_bucket)
        .collect::<Vec<_>>();
    let first = engine.create_bucket_dump_manifest(1, buckets).unwrap();
    let expected = locked_result(&engine, 127, true);
    let shared = latest_bucket_dump_manifest_shared_at(&engine.index_dir, 1).unwrap();
    let users = Arc::strong_count(&shared);
    let snapshot = engine.capture_bucket_storage_summaries(1, true).unwrap();
    assert_eq!(
        Arc::strong_count(&shared),
        users,
        "capture pins a full index image"
    );
    assert_eq!(snapshot.summaries, expected.0);
    write(&engine);
    let second = engine
        .create_bucket_dump_manifest(1, first.bucket_ids.clone())
        .unwrap();
    assert_ne!(first.manifest_id, second.manifest_id);
    assert_eq!(snapshot.summaries, expected.0);
    assert_eq!(snapshot.finish(), expected);
}

#[test]
fn bucket_summary_snapshot_does_not_cover_a_later_generation() {
    let dir = tempfile::tempdir().unwrap();
    let engine = open(dir.path(), 127);
    let buckets = engine
        .bucket_storage_summaries(1)
        .into_iter()
        .map(|row| row.routing_bucket)
        .collect::<Vec<_>>();
    engine.create_bucket_dump_manifest(1, buckets).unwrap();
    let snapshot = engine.capture_bucket_storage_summaries(1, true).unwrap();
    write(&engine);
    let (old_summaries, old_digests) = snapshot.finish();
    let (current_summaries, current_digests) =
        engine.bucket_storage_summaries_with_digests(1, true);
    let changed = old_digests
        .iter()
        .filter_map(|(bucket, digest)| {
            (current_digests.get(bucket) != Some(digest)).then_some(*bucket)
        })
        .collect::<Vec<_>>();
    assert!(!changed.is_empty());
    let plan = engine.storage_wal_reclaim_plan(1, [], []);
    let newest = engine
        .write_ahead_log_store()
        .stats_without_rescan(1)
        .last_sequence;
    assert!(newest > 0);
    // An uncovered dirty bucket may name its own retention floor instead of blocking
    // the entire plan. Either outcome must keep the newly written record.
    assert!(
        plan.retain_from_wal_sequence <= newest,
        "newest={newest} plan={plan:?}"
    );
    for bucket in changed {
        let old = old_summaries
            .iter()
            .find(|row| row.routing_bucket == bucket)
            .unwrap();
        let current = current_summaries
            .iter()
            .find(|row| row.routing_bucket == bucket)
            .unwrap();
        assert!(!bucket_dump_summary_matches_current_generation(
            old,
            current,
            &old_digests,
            &current_digests
        ));
    }
}
