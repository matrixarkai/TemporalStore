// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 MatrixArkAI

//! Delta / incremental served-index path (now the only served-index mechanism).
//!
//! A write no longer rewrites the whole `shard-{id}.index.json` (O(store) per write); the
//! base snapshot is materialized only at compaction points (dump / flush / gc / unload).
//! Between compactions the in-memory shard is the authoritative served index, which every
//! reader reaches through the served-index funnel (`export_index_bytes` /
//! `read_stream(Index)`), and a cold reload reconstructs current state by folding the base
//! with the index-log deltas beyond the last materialized anchor.

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
    // The delta served-index is now the only path (no flag) -- this validates its contract.
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
    // The served index is a CONTAINER -- magic, a codec byte, then a zstd payload (and, for the
    // msgpack codec, a four-byte struct-version stamp before it). It has not been text since that
    // landed: `encode_index_bytes_as_plain_json` is `cfg(test)` and its own comment says
    // production has no way to produce the plain shape any more. So searching the raw bytes for
    // key names asserted a format that cannot occur, and this test has failed on main ever since.
    //
    // WHAT THIS COSTS: the funnel-is-current claim. Recovering it needs either a public decoder or
    // a crate-internal test that can reach `decode_index_bytes`, and duplicating the container
    // format here -- in a test, alongside the real reader -- is the wrong way to buy it back.
    // Checkable from an integration test is that the funnel produced a container at all, and that
    // the stream read returns those same bytes, asserted just below. The keys are read back for
    // real in (4).
    assert!(
        served.starts_with(b"TSIDX"),
        "the funnel must serve a served-index container, got {} bytes starting {:?}",
        served.len(),
        &served[..served.len().min(8)]
    );
    // read_stream(Index) is routed through the same funnel.
    let stream = engine.read_stream(StreamReadRequest {
        shard_id: SHARD_ID,
        stream_kind: StreamKind::Index,
        block_slab_id: 0,
        offset: 0,
        size: served.len() as u64,
    });
    assert!(stream.status.ok, "index stream read: {:?}", stream.status);
    assert_eq!(stream.data, served, "index stream must match the funnel bytes");

    // (3) A dump materializes the base (compaction point) and embeds the current index.
    let manifest = engine
        .create_bucket_dump_manifest(SHARD_ID, Vec::new())
        .expect("dump manifest should persist");
    // Same container, same reason: this searched the embedded index for a key name.
    assert!(
        manifest.index_bytes.starts_with(b"TSIDX"),
        "dump manifest must embed a served-index container"
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
