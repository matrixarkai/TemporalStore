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

use temporalstore_rust::{Command, CommandResponse, ExecuteRequest, TemporalEngine};

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

// The index-log lives under `<index_dir>/indexlogs/shard-{id}.indexlog.jsonl`.
fn index_log_len(root: &Path) -> u64 {
    fs::metadata(
        root.join("indexes")
            .join("indexlogs")
            .join(format!("shard-{SHARD}.indexlog.jsonl")),
    )
    .map(|m| m.len())
    .unwrap_or(0)
}

#[test]
fn crash_before_any_dump_reconstructs_from_full_delta_log() {
    // No dump ever happens: the base is absent, the whole write history lives in the
    // index-log. Reload must fold the full delta log onto an empty base.
    let root = root("nodump");
    let _ = fs::remove_dir_all(&root);
    let engine = build(&root);
    engine.load_shard(SHARD);
    for i in 0..30 {
        set(&engine, &format!("k{i:03}"), &format!("val{i}"));
    }
    assert!(
        index_log_len(&root) > 0,
        "index-log must carry the deltas before any dump (always-on)"
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
