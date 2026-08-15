// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Delta / incremental served-index path (`MATRIXARK_DELTA_SERVED_INDEX`).
//!
//! On the delta path a write no longer rewrites the whole `shard-{id}.index.json`
//! (O(store) per write); the base snapshot is materialized only at compaction points
//! (dump / flush / gc / unload-via-WAL). Between compactions the in-memory shard is the
//! authoritative served index, which every reader reaches through the served-index funnel
//! (`export_index_bytes` / `read_stream(Index)`), and a cold reload reconstructs current
//! state by replaying the WAL suffix beyond the last materialized anchor.
//!
//! Runs in its own integration-test process, so toggling the process-wide
//! `MATRIXARK_DELTA_SERVED_INDEX` env var cannot race the single-process library unit tests.

use std::path::PathBuf;

use temporalstore_rust::{
    Command, CommandResponse, ExecuteRequest, StreamKind, StreamReadRequest, TemporalEngine,
};

const SHARD_ID: u64 = 1;

fn root(tag: &str) -> PathBuf {
    let mut root = std::env::temp_dir();
    root.push(format!("ts-delta-served-index-{tag}-{}", std::process::id()));
    root
}

fn build_engine(root: &PathBuf) -> (TemporalEngine, PathBuf) {
    let _ = std::fs::remove_dir_all(root);
    let index_dir = root.join("indexes");
    for sub in ["cache", "pages", "indexes"] {
        std::fs::create_dir_all(root.join(sub)).expect("create engine dir");
    }
    let engine =
        TemporalEngine::with_local_dirs(4096, root.join("cache"), root.join("pages"), &index_dir);
    (engine, index_dir)
}

fn set(engine: &TemporalEngine, key: &str, value: &[u8]) {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::StringSet {
            key: key.to_string(),
            value: value.to_vec(),
        },
    });
    assert!(response.status.ok, "set {key} failed: {response:?}");
}

fn get(engine: &TemporalEngine, key: &str) -> Option<Vec<u8>> {
    let response = engine.execute(ExecuteRequest {
        shard_id: SHARD_ID,
        command: Command::StringGet {
            key: key.to_string(),
        },
    });
    assert!(response.status.ok, "get {key} failed: {response:?}");
    match response.response {
        CommandResponse::Bytes { value } => value,
        other => panic!("unexpected response for get {key}: {other:?}"),
    }
}

#[test]
fn delta_path_defers_base_write_but_funnel_and_reload_see_current_state() {
    std::env::set_var("MATRIXARK_DELTA_SERVED_INDEX", "1");
    // Guard so a panic does not leak the env var into any later test in this process.
    struct Reset;
    impl Drop for Reset {
        fn drop(&mut self) {
            std::env::remove_var("MATRIXARK_DELTA_SERVED_INDEX");
        }
    }
    let _reset = Reset;

    let root = root("roundtrip");
    let (engine, index_dir) = build_engine(&root);
    let index_path = index_dir.join(format!("shard-{SHARD_ID}.index.json"));
    engine.load_shard(SHARD_ID);

    for key in ["alpha", "bravo", "charlie"] {
        set(&engine, key, b"v1");
    }

    // (1) O(delta) per write: the whole-index base file was NOT rewritten per write. On the
    // delta path the sync execute path skips the per-write base rewrite entirely, so with no
    // compaction yet the base file is still absent.
    assert!(
        !index_path.exists(),
        "delta path must not rewrite the base index per write"
    );

    // (2) The funnel still serves the COMPLETE, current index from the live shard.
    let served = engine
        .export_index_bytes(SHARD_ID)
        .expect("funnel serves the live index");
    let served_text = String::from_utf8_lossy(&served);
    for key in ["alpha", "bravo", "charlie"] {
        assert!(
            served_text.contains(key),
            "served index must contain {key}: {served_text}"
        );
    }
    // read_stream(Index) is routed through the same funnel.
    let stream = engine.read_stream(StreamReadRequest {
        shard_id: SHARD_ID,
        stream_kind: StreamKind::Index,
        page_slab_id: 0,
        offset: 0,
        size: served.len() as u64,
    });
    assert!(stream.status.ok, "index stream read: {:?}", stream.status);
    assert_eq!(stream.data, served, "index stream must match the funnel bytes");

    // (3) A dump materializes the base (compaction point) and embeds the current index.
    let manifest = engine
        .create_bucket_dump_manifest(SHARD_ID, Vec::new())
        .expect("dump manifest should persist");
    assert!(
        String::from_utf8_lossy(&manifest.index_bytes).contains("charlie"),
        "dump manifest must embed the current served index"
    );

    // (4) Durability across a cold reload: the deferred writes live in the WAL and are
    // replayed on load, so no data is lost even though the base was never rewritten per write.
    engine.unload_shard(SHARD_ID);
    engine.load_shard(SHARD_ID);
    for key in ["alpha", "bravo", "charlie"] {
        assert_eq!(
            get(&engine, key).as_deref(),
            Some(b"v1".as_ref()),
            "reload must reconstruct {key} from the WAL suffix"
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
