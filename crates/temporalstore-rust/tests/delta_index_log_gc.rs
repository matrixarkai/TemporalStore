// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Delta served-index crash reconstruction across the index-log + base-snapshot boundary.
//!
//! The index-log is now always-on and is the delta stream the load-time fold replays onto
//! the base snapshot. A crash BEFORE any compaction reconstructs from the full delta log; a
//! crash AFTER a dump (which anchors a durable base via the manifest, installed on load)
//! plus further writes reconstructs fold(base + retained deltas). The index-log itself is
//! bounded by the consumer-aware storage-manager index GC, which is exercised by the lib
//! test `storage_wal_index_gc_reclaim_requires_durable_generation_and_retention_release`
//! (records removed + budget + restart reconstruction).

use std::fs;
use std::path::{Path, PathBuf};

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, LocalIndexLogStore, TemporalEngine,
};

const SHARD: u64 = 1;

fn root(tag: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-idxlog-gc-{tag}-{}", std::process::id()));
    root
}

fn build(root: &Path) -> TemporalEngine {
    for sub in ["cache", "pages", "indexes"] {
        fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    TemporalEngine::with_local_dirs(4096, root.join("cache"), root.join("pages"), root.join("indexes"))
}

fn set(engine: &TemporalEngine, key: &str, value: &str) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.as_bytes().to_vec(),
        },
    });
    assert!(response.status.ok, "set {key}: {response:?}");
}

fn get(engine: &TemporalEngine, key: &str) -> Option<String> {
    match engine
        .execute(ExecuteRequest {
            shard_id: SHARD,
            command: Command::StringGet {
                key: key.to_string(),
            },
        })
        .response
    {
        CommandResponse::Bytes { value } => value.map(|v| String::from_utf8_lossy(&v).to_string()),
        other => panic!("unexpected get response for {key}: {other:?}"),
    }
}

// Where the engine keeps its index-logs: `LocalIndexLogStore::new(index_dir.join("indexlogs"))`
// in `engine::lifecycle`.
fn index_log_dir(root: &Path) -> PathBuf {
    root.join("indexes").join("indexlogs")
}

/// Bytes of the shard's index-log, counted the way the store counts its own.
///
/// `log_len_bytes` walks `index_log_segment_paths`, which is the enumeration every reader of
/// this log shares, so this cannot disagree with the store about which files are the log.
/// Do NOT spell a filename here. This helper used to stat
/// `shard-{id}.indexlog.jsonl` directly, and when the pieces were renamed to `.bin` it stopped
/// finding anything -- for six days, silently, because it turned "no such file" into a zero via
/// `unwrap_or(0)`. Naming a file also cannot be right in both rolling configurations: a shard's
/// log is one file when `TS_INDEX_LOG_SEGMENT_BYTES` is 0 and a run of sealed pieces named
/// `shard-{id}.indexlog.{start}-{end}-{anchor}.bin` otherwise, and asking the store covers both.
///
/// Only stats, never opens: `piece_count` and `log_len_bytes` read metadata, so calling them
/// against a live engine's directory cannot disturb an append in flight. (`record_count` would
/// -- its scan trims a torn tail -- which is why the count below is of pieces and bytes.)
fn index_log_on_disk(root: &Path) -> (usize, u64) {
    let dir = index_log_dir(root);
    let store = LocalIndexLogStore::new(&dir);
    let pieces = store.piece_count(SHARD);
    // The loud not-found case. The old helper answered 0 here, which is also what a log that
    // exists and carries nothing answers, so the failure could not say which of the two had
    // happened -- and a rename sat behind that 0 for six days. The listing is what separates
    // them, so branch on it and say which one this is.
    if pieces == 0 {
        let present = dir_listing(&dir);
        assert!(
            !present.is_empty(),
            "no index-log at all for shard {SHARD}: {} holds no files, so nothing wrote a delta. \
             This is the writer's side, not the name's.",
            dir.display()
        );
        panic!(
            "the store enumerates 0 index-log pieces for shard {SHARD} in {}, which holds {} \
             file(s): {present:?}. The files are there and the enumeration does not recognise \
             them -- writer and reader disagree about the name. This is the shape of the rename \
             to `.bin` that left this test looking for `.jsonl`.",
            dir.display(),
            present.len()
        );
    }
    (pieces, store.log_len_bytes(SHARD))
}

/// Every name in the index-log directory, for the message above. Sorted, so it reads the same
/// way twice.
fn dir_listing(dir: &Path) -> Vec<String> {
    let mut names = fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    names.sort();
    names
}

#[test]
fn crash_before_any_dump_reconstructs_from_full_delta_log() {
    // No dump ever happens: the base is absent, the whole write history lives in the
    // index-log. Reload must fold the full delta log onto an empty base.
    let root = root("nodump");
    let _ = fs::remove_dir_all(&root);
    let engine = build(&root);
    engine.load_shard(SHARD);
    // The first write on its own, so the growth check below has a NON-ZERO baseline. Against a
    // fresh root `bytes > 0` and `bytes > before` are the same statement -- before is 0 -- and
    // the weaker of the two is satisfied by the log merely existing. Measuring after one write
    // and again after thirty makes the assertion say the later writes reached the log.
    set(&engine, "k000", "val0");
    let (pieces_after_one, after_one) = index_log_on_disk(&root);
    assert!(
        after_one > 0,
        "one set must put bytes in the index-log: {pieces_after_one} piece(s) hold {after_one} \
         bytes"
    );
    for i in 1..30 {
        set(&engine, &format!("k{i:03}"), &format!("val{i}"));
    }
    let (pieces, bytes) = index_log_on_disk(&root);
    assert!(
        bytes > after_one,
        "index-log must carry the deltas before any dump (always-on): the 29 writes after the \
         first took it from {after_one} bytes over {pieces_after_one} piece(s) to {bytes} bytes \
         over {pieces} piece(s)"
    );
    drop(engine);

    let reopened = build(&root);
    reopened.load_shard(SHARD);
    for i in 0..30 {
        assert_eq!(
            get(&reopened, &format!("k{i:03}")).as_deref(),
            Some(format!("val{i}").as_str()),
            "crash before any dump must reconstruct k{i:03} from the full delta log"
        );
    }
    let _ = fs::remove_dir_all(&root);
}

#[test]
fn crash_after_dump_plus_writes_reconstructs_from_base_plus_retained_deltas() {
    // Dump anchors a durable base (manifest, installed on reload); further writes land only
    // in the retained index-log deltas. Reload must fold base + those deltas.
    let root = root("postdump");
    let _ = fs::remove_dir_all(&root);
    let engine = build(&root);
    engine.load_shard(SHARD);
    for i in 0..40 {
        set(&engine, &format!("k{i:03}"), "pre-dump");
    }
    engine
        .create_bucket_dump_manifest(SHARD, Vec::<u32>::new())
        .expect("dump should succeed");
    // Overwrite half the keys AFTER the dump -- these live only in post-anchor deltas.
    for i in 0..20 {
        set(&engine, &format!("k{i:03}"), "post-dump");
    }
    drop(engine);

    let reopened = build(&root);
    reopened.load_shard(SHARD);
    for i in 0..20 {
        assert_eq!(
            get(&reopened, &format!("k{i:03}")).as_deref(),
            Some("post-dump"),
            "post-dump overwrite of k{i:03} must survive via fold(base + retained deltas)"
        );
    }
    for i in 20..40 {
        assert_eq!(
            get(&reopened, &format!("k{i:03}")).as_deref(),
            Some("pre-dump"),
            "pre-dump value of k{i:03} must survive in the installed base snapshot"
        );
    }
    let _ = fs::remove_dir_all(&root);
}
