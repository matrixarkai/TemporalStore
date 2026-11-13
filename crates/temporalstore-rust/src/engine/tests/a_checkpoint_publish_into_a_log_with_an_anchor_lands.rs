// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! A VISIBILITY PUBLISH INTO A LOG THAT ALREADY HOLDS ITS OWN ANCHOR STILL LANDS.
//!
//! `publish_shard_index_snapshot_for_keys` is the call behind the proxy's publish-visibility
//! endpoint. When its checkpoint is due it appends an ANCHOR to the index log (`append_index_bytes`)
//! and returns `publish_visibility_failed` if that append fails. The append first rolls the piece
//! being written if it has passed the rolling threshold, and rolling reads the piece to name it.
//! That read used to take any record that was not a whole-index record for a delta, so once the
//! piece held an anchor -- the one the previous checkpoint wrote -- the next roll failed with an
//! Encoding error and the publish failed with it.
//!
//! This drives it the way production reaches it and not through the index log's own API: a store
//! opened on real directories, writes through the async-storage arm (the only one that mints
//! log-resident blocks, which a publish exists to move), and rounds of one write and one checkpoint
//! publish of it, each of which appends an anchor, until the piece being written is past the default
//! rolling threshold -- then one more. The checkpoint interval is set to 1 so that each publish is
//! due; in production a publish is due after the interval's worth of published keys instead.
//!
//! THE REGIME IS ASSERTED. Every publish must report bytes (it found something log-resident and
//! checkpointed), the log must hold at least a thousand anchors, and the piece the last publish
//! appends to must be past the threshold -- otherwise it would land for the trivial reason that
//! nothing needed rolling.

#![allow(clippy::all)]
use super::*;

const SHARD: u64 = 31;

fn key_at(item: usize) -> String {
    format!("anchored-{item:06}")
}

/// Write one key THROUGH THE ASYNC-STORAGE ARM, as the production publish caller does.
fn seed_one(engine: &TemporalEngine, item: usize) {
    let response = engine.execute_replicated(
        ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringSet {
                key: key_at(item),
                value: vec![b'v'; 128],
            },
        }
        .with_async_storage(),
    );
    assert!(response.status.ok, "seed must ack at {item}: {response:?}");
}

fn active_log_len(index_dir: &std::path::Path) -> u64 {
    std::fs::metadata(index_dir.join("indexlogs").join(format!("shard-{SHARD}.indexlog.bin")))
        .map(|meta| meta.len())
        .unwrap_or(0)
}

/// How many anchor records the shard's index log files hold, across every piece.
fn anchors_in_log(index_dir: &std::path::Path) -> usize {
    let mut anchors = 0usize;
    for entry in std::fs::read_dir(index_dir.join("indexlogs")).expect("the index log directory exists") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with(&format!("shard-{SHARD}.indexlog.")) {
            continue;
        }
        let mut reader = std::io::BufReader::new(std::fs::File::open(entry.path()).expect("open"));
        while let Some((_, payload)) = crate::log_framing::read_frame(&mut reader).expect("frame") {
            if crate::index_log::index_payload_shape(&payload)
                == Some(crate::index_log::INDEX_LOG_SHAPE_ANCHOR)
            {
                anchors += 1;
            }
        }
    }
    anchors
}

#[test]
fn a_second_checkpoint_publish_lands_in_a_log_that_holds_the_first_ones_anchor() {
    let dir = tempfile::tempdir().expect("tempdir");
    let index_dir = dir.path().join("indexes");
    let engine = TemporalEngine::with_local_dirs(
        64,
        dir.path().join("cache"),
        dir.path().join("blocks"),
        index_dir.clone(),
    );
    engine.load_shard(SHARD);
    engine.set_visibility_publish_index_flush_interval_for_test(1);
    assert_eq!(engine.visibility_publish_index_flush_interval(), 1);

    // EVERY CHECKPOINT PUBLISH APPENDS AN ANCHOR, and an anchor is all some of these pieces hold:
    // the piece grows by them. One write and one publish of it per round, until the piece being
    // written is past the default rolling threshold. Every publish on the way must land.
    seed_one(&engine, 0);
    let first = engine
        .publish_shard_index_snapshot_for_keys(SHARD, vec![key_at(0)])
        .expect("the first checkpoint publish must land");
    assert!(
        first > 0,
        "the first publish reported 0 bytes, so it found nothing log-resident and wrote no anchor"
    );
    let threshold = crate::index_log::DEFAULT_INDEX_LOG_SEGMENT_BYTES;
    let mut item = 0usize;
    while active_log_len(&index_dir) < threshold {
        item += 1;
        assert!(
            item < 10_000,
            "denominator: {item} publishes never took the piece past {threshold} B (it is {} B)",
            active_log_len(&index_dir)
        );
        seed_one(&engine, item);
        let wrote = engine
            .publish_shard_index_snapshot_for_keys(SHARD, vec![key_at(item)])
            .unwrap_or_else(|status| {
                panic!("a checkpoint publish before the piece was full failed at round {item}: {status:?}")
            });
        assert!(wrote > 0, "round {item}: the publish reported 0 bytes, so it wrote no anchor");
    }
    let anchors = anchors_in_log(&index_dir);
    println!(
        "  piece being written: {} B after {item} rounds; anchors in the log: {anchors}",
        active_log_len(&index_dir)
    );
    assert!(
        anchors >= 1_000,
        "denominator: only {anchors} anchors, so the piece did not grow by them"
    );

    // The piece being written is past the threshold and holds anchors: this publish's anchor
    // append has to roll it.
    item += 1;
    seed_one(&engine, item);
    let second = engine.publish_shard_index_snapshot_for_keys(SHARD, vec![key_at(item)]);
    println!("  publish that rolls a piece holding anchors -> {second:?}");
    let second = second.expect(
        "the checkpoint publish whose anchor append rolls a piece that holds anchors must land",
    );
    assert!(second > 0, "the rolling publish reported 0 bytes");

    // And what a restart would read: the fold passes over the anchors.
    let folded = engine
        .index_log_store()
        .read_delta_records(SHARD, 0)
        .expect("the log holding anchors must fold");
    println!("  the fold read {} delta records and passed over the anchors", folded.len());
}
