// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! The serving gate the two restore paths are supposed to shut while they replay.
//!
//! A restore is an index swap plus a WAL replay, and between those two the shard describes
//! itself as it was AT THE CHECKPOINT. `recovering` is what keeps that window private: `execute`,
//! the stream-batch apply, the storage-manager cycle and the reclaim RPC all read it and refuse.
//!
//! Both production sites that shut it -- `load_shard_with` and `install_bucket_dump_manifest` --
//! were unguarded. Every test that reached the flag published the info row itself through
//! `test_publish_recovering_shard`, so the value under test was the one the test had just written.
//! Setting either site to publish `recovering: false` left the whole library suite green.
//!
//! These two guards read `REPLAY_ENTERED_WITH_SERVING_GATE_SHUT`, which the replay entry point
//! records from the engine's own `shard_is_recovering` before it applies a record -- so the
//! subject is the shipping code path, and the fixture has no way to supply the answer.
#![allow(clippy::all)]
use super::*;

use crate::engine::lifecycle::{LAST_REPLAY_WATERMARK, REPLAY_ENTERED_WITH_SERVING_GATE_SHUT};
use std::sync::atomic::Ordering::SeqCst;

/// Both guards below read one process-wide probe, so they must not overlap.
static PROBE: std::sync::Mutex<()> = std::sync::Mutex::new(());

const SHARD: ShardId = 31;

fn write_strings(engine: &TemporalEngine, prefix: &str, count: usize) {
    for index in 0..count {
        let response = engine.execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: format!("{prefix}-{index}"),
                value: format!("{prefix}-value-{index}").into_bytes(),
            },
        });
        assert!(response.status.ok, "write {prefix}-{index}: {response:?}");
    }
}

/// How many WAL records sit above `watermark`. The denominator for every "the replay had work"
/// claim below: a replay with nothing to do runs with the gate in any state at all and proves
/// nothing about the window.
fn suffix_above(engine: &TemporalEngine, watermark: u64) -> usize {
    let (records, _truncated) = engine
        .wal_store()
        .scan_decoded(SHARD, 0, u64::MAX, u64::MAX)
        .expect("scan wal");
    records
        .iter()
        .filter(|(_, record)| record.sequence > watermark)
        .count()
}

/// THE LOAD PATH. `load_shard_with` inserts into the `shards` map before it replays, deliberately,
/// so a concurrent writer can see the shard and be refused -- which only works if the info row it
/// publishes first carries `recovering: true`.
#[test]
fn a_load_that_replays_a_wal_suffix_runs_with_the_serving_gate_shut() {
    let _held = PROBE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let index_dir = dir.path().join("indexes");

    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        &index_dir,
    );
    engine.load_shard(SHARD);
    write_strings(&engine, "pre", 6);
    engine.wal_store().flush(SHARD).expect("flush wal");
    drop(engine);

    let restarted = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("restart-cache"),
        dir.path().join("pages"),
        &index_dir,
    );
    // Cleared so the assertion below cannot be satisfied by some earlier replay in this process.
    REPLAY_ENTERED_WITH_SERVING_GATE_SHUT.store(false, SeqCst);
    restarted.load_shard(SHARD);

    let watermark = LAST_REPLAY_WATERMARK.load(SeqCst);
    let suffix = suffix_above(&restarted, watermark);
    assert!(
        suffix > 0,
        "VACUITY: the restart replayed nothing (watermark {watermark}), so the gate was never \
         load-bearing and this guard would pass whatever the flag said"
    );
    assert!(
        REPLAY_ENTERED_WITH_SERVING_GATE_SHUT.load(SeqCst),
        "load_shard_with published the shard with serving OPEN and then replayed {suffix} WAL \
         records into it: a read in that window answers from the pre-replay index and a write \
         interleaves with replay"
    );
    // CONTROL: the probe is not simply stuck true -- the gate is open again once replay is done,
    // which is also what makes the shard servable at all.
    assert!(
        !restarted.shard_is_recovering(SHARD),
        "a completed load must reopen serving"
    );
    let response = restarted.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: "pre-0".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"pre-value-0".to_vec())
        },
        "the recovered shard must serve what it replayed"
    );
}

/// THE INSTALL PATH, which is the one the dump-install endpoint drives against a RUNNING shard.
/// The index swapped in describes the shard at the dump; the records written after it live only
/// in the log until the replay underneath puts them back. The gate is what keeps that window from
/// being served, and nothing in the crate read it on this path.
///
/// Installed into a restore target -- the dump's pages, a fresh index dir, the log -- because a
/// manifest re-installed onto the shard that minted it is refused as stale by its own index-log
/// sequence, which is not this window.
#[test]
fn a_dump_install_onto_a_loaded_shard_replays_with_the_serving_gate_shut() {
    let _held = PROBE.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = tempfile::tempdir().unwrap();
    let source_index_dir = dir.path().join("indexes");

    let engine = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("cache"),
        dir.path().join("pages"),
        &source_index_dir,
    );
    engine.load_shard(SHARD);
    write_strings(&engine, "pre", 6);
    let manifest = engine
        .create_bucket_dump_manifest(SHARD, Vec::new())
        .expect("dump manifest should persist");
    write_strings(&engine, "post", 4);
    engine.wal_store().flush(SHARD).expect("flush wal");

    let suffix = suffix_above(&engine, manifest.wal_sequence);
    assert!(
        suffix > 0,
        "VACUITY: no post-dump suffix (anchor {}), so installing onto the loaded shard has \
         nothing to replay and the window this guards does not exist in the fixture",
        manifest.wal_sequence
    );

    let restore_index_dir = dir.path().join("restore-indexes");
    std::fs::create_dir_all(&restore_index_dir).unwrap();
    copy_tree(
        &source_index_dir.join("wals"),
        &restore_index_dir.join("wals"),
    );
    let restored = TemporalEngine::with_local_dirs(
        1024 * 1024,
        dir.path().join("restore-cache"),
        dir.path().join("pages"),
        &restore_index_dir,
    );
    // The already-loaded shard the install lands on.
    restored.load_shard(SHARD);

    // Cleared AFTER that load, so only the install's own replay can satisfy the assertion.
    REPLAY_ENTERED_WITH_SERVING_GATE_SHUT.store(false, SeqCst);
    restored
        .install_bucket_dump_manifest(&manifest)
        .expect("manifest should install onto the loaded restore target");

    assert!(
        REPLAY_ENTERED_WITH_SERVING_GATE_SHUT.load(SeqCst),
        "install_bucket_dump_manifest swapped the index of a LOADED shard with serving OPEN and \
         then replayed {suffix} WAL records into it: reads in that window answer with the \
         post-dump records missing"
    );
    assert!(
        !restored.shard_is_recovering(SHARD),
        "a completed install must reopen serving"
    );
    let response = restored.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringGet {
            key: "post-3".to_string(),
        },
    });
    assert_eq!(
        response.response,
        CommandResponse::Bytes {
            value: Some(b"post-value-3".to_vec())
        },
        "the install must leave the post-dump suffix readable"
    );
}

fn copy_tree(from: &std::path::Path, to: &std::path::Path) {
    std::fs::create_dir_all(to).expect("create copy target");
    for entry in std::fs::read_dir(from).expect("read copy source") {
        let entry = entry.expect("copy source entry");
        let target = to.join(entry.file_name());
        if entry.file_type().expect("file type").is_dir() {
            copy_tree(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), &target).expect("copy file");
        }
    }
}
